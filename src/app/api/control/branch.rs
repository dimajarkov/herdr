//! Server-owned ancestry and accepted-order placement for managed branches.
use super::*;
use crate::app::control::{BranchMember, ManagedBranch};
use crate::layout::{Node, PaneId, TileLayout};
use ratatui::layout::Direction;

pub(super) struct BranchLaunchPlan {
    pub layout: TileLayout,
    pub branch: ManagedBranch,
    pub parent_launch: String,
    pub terminal_size: (u16, u16),
}

impl App {
    pub(in crate::app::api) fn handle_managed_branch_launch(
        &mut self,
        id: String,
        params: crate::api::schema::ManagedBranchLaunchParams,
    ) -> String {
        let plan = match self.plan_managed_branch(&params) {
            Ok(plan) => plan,
            Err(code) => return encode_error(id, code, "branch placement refused before allocation; verify runtime ownership, unzoom and provide at least 40 columns by 12 terminal rows per managed pane"),
        };
        self.handle_managed_job_launch_with_layout(
            id,
            ManagedLaunchParams {
                parent: params.parent,
                argv: params.argv,
                cwd: params.cwd,
                env: params.env,
                direction: crate::api::schema::SplitDirection::Right,
                label: params.label,
            },
            Some(plan),
        )
    }

    fn plan_managed_branch(
        &self,
        params: &crate::api::schema::ManagedBranchLaunchParams,
    ) -> Result<BranchLaunchPlan, &'static str> {
        self.validate_terminal_condition(&params.parent)?;
        self.validate_terminal_condition(&params.root)?;
        let (ws, parent) = self
            .parse_current_public_pane_id(&params.parent.pane_id)
            .ok_or("terminal_not_found")?;
        let tab = self.state.workspaces[ws]
            .find_tab_index_for_pane(parent)
            .ok_or("terminal_not_found")?;
        let target = &self.state.workspaces[ws].tabs[tab];
        if target.zoomed {
            return Err("layout_zoomed");
        }
        let (root_ws, root) = self
            .parse_current_public_pane_id(&params.root.pane_id)
            .ok_or("terminal_not_found")?;
        if root_ws != ws || self.state.workspaces[ws].find_tab_index_for_pane(root) != Some(tab) {
            return Err("branch_root_mismatch");
        }
        let mut branch = self
            .managed_jobs
            .branches
            .get(&params.root.launch_id)
            .cloned()
            .unwrap_or_else(|| ManagedBranch {
                root: params.root.clone(),
                members: Vec::new(),
            });
        if branch.root != params.root {
            return Err("branch_root_mismatch");
        }
        // A descendant cannot declare itself to be a fresh master and annex its region.
        if self.managed_jobs.branches.values().any(|other| {
            other
                .members
                .iter()
                .any(|m| m.condition.terminal.launch_id == params.root.launch_id)
        }) {
            return Err("branch_root_mismatch");
        }
        if params.parent != params.root {
            let member = branch
                .members
                .iter()
                .find(|m| m.condition.terminal == params.parent)
                .ok_or("branch_parent_mismatch")?;
            self.validate_managed_job(&member.condition)?;
        }
        let mut visible = std::collections::HashMap::new();
        for member in &mut branch.members {
            if self
                .managed_jobs
                .jobs
                .get(&member.condition.job_id)
                .is_some_and(|job| {
                    matches!(
                        job.info.phase,
                        ManagedJobPhase::Released
                            | ManagedJobPhase::Moved
                            | ManagedJobPhase::Replaced
                    )
                })
            {
                return Err("branch_layout_changed");
            }
            match self.validate_terminal_condition(&member.condition.terminal) {
                Ok(()) => {
                    let (member_ws, pane) = self
                        .parse_current_public_pane_id(&member.condition.terminal.pane_id)
                        .ok_or("branch_layout_changed")?;
                    if member_ws != ws
                        || self.state.workspaces[ws].find_tab_index_for_pane(pane) != Some(tab)
                    {
                        return Err("branch_layout_changed");
                    }
                    visible.insert(member.condition.terminal.launch_id.clone(), pane);
                }
                Err(_) => {
                    let retired = member.retired
                        || self
                            .managed_jobs
                            .jobs
                            .get(&member.condition.job_id)
                            .is_some_and(|job| {
                                matches!(
                                    job.info.phase,
                                    ManagedJobPhase::Closed | ManagedJobPhase::Exited
                                )
                            });
                    member.retired = retired;
                    if !retired
                        || self
                            .parse_current_public_pane_id(&member.condition.terminal.pane_id)
                            .is_some()
                    {
                        return Err("branch_layout_changed");
                    }
                }
            }
        }
        // Keep only live members and their ancestral ordering anchors.
        let mut retained: std::collections::HashSet<_> = visible.keys().cloned().collect();
        loop {
            let before = retained.len();
            for member in &branch.members {
                if retained.contains(&member.condition.terminal.launch_id) {
                    retained.insert(member.parent_launch.clone());
                }
            }
            if retained.len() == before {
                break;
            }
        }
        branch
            .members
            .retain(|member| retained.contains(&member.condition.terminal.launch_id));
        if branch.members.len() >= 256 {
            return Err("branch_limit");
        }
        let mut columns = vec![vec![root]];
        for member in branch
            .members
            .iter()
            .filter(|m| m.parent_launch == branch.root.launch_id)
        {
            let mut column = Vec::new();
            collect_descendants(
                &branch,
                &member.condition.terminal.launch_id,
                &visible,
                &mut column,
            );
            if !column.is_empty() {
                columns.push(column);
            }
        }
        let old_order: Vec<_> = columns.iter().flatten().copied().collect();
        // A placeholder is replaced by the allocated pane only after spawn succeeds.
        let pending = PaneId::from_raw(0);
        if params.parent == params.root {
            columns.push(vec![pending]);
        } else {
            let mut with_pending = visible.clone();
            with_pending.insert("pending".into(), pending);
            let mut candidate_branch = branch.clone();
            candidate_branch.members.push(BranchMember {
                condition: crate::api::schema::JobCondition {
                    terminal: crate::api::schema::TerminalCondition {
                        launch_id: "pending".into(),
                        ..params.parent.clone()
                    },
                    job_id: String::new(),
                },
                parent_launch: params.parent.launch_id.clone(),
                retired: false,
            });
            columns = vec![vec![root]];
            for member in candidate_branch
                .members
                .iter()
                .filter(|m| m.parent_launch == branch.root.launch_id)
            {
                let mut column = Vec::new();
                collect_descendants(
                    &candidate_branch,
                    &member.condition.terminal.launch_id,
                    &with_pending,
                    &mut column,
                );
                if !column.is_empty() {
                    columns.push(column);
                }
            }
        }
        let replacement = sequence(
            Direction::Horizontal,
            columns
                .into_iter()
                .map(|column| {
                    sequence(
                        Direction::Vertical,
                        column.into_iter().map(Node::Pane).collect(),
                    )
                })
                .collect(),
        );
        let mut layout = target.layout.clone();
        if !layout.replace_region(&old_order, replacement) {
            return Err("branch_layout_changed");
        }
        let sizes = self.state.new_layout_pane_sizes(&layout);
        let ids = layout.pane_ids();
        // Existing chrome/gutter sizing is authoritative. The floor exceeds PTY safety clamping.
        if ids.iter().zip(&sizes).any(|(pane, (rows, cols))| {
            (old_order.contains(pane) || *pane == pending) && (*rows < 12 || *cols < 40)
        }) {
            return Err("layout_capacity");
        }
        let terminal_size = ids
            .iter()
            .zip(sizes)
            .find_map(|(pane, size)| (*pane == pending).then_some(size))
            .ok_or("branch_layout_changed")?;
        Ok(BranchLaunchPlan {
            terminal_size,
            layout,
            branch,
            parent_launch: params.parent.launch_id.clone(),
        })
    }
}

fn collect_descendants(
    branch: &ManagedBranch,
    launch: &str,
    visible: &std::collections::HashMap<String, PaneId>,
    out: &mut Vec<PaneId>,
) {
    if let Some(pane) = visible.get(launch) {
        out.push(*pane);
    }
    for child in branch.members.iter().filter(|m| m.parent_launch == launch) {
        collect_descendants(branch, &child.condition.terminal.launch_id, visible, out);
    }
}

fn sequence(direction: Direction, mut nodes: Vec<Node>) -> Node {
    assert!(!nodes.is_empty());
    if nodes.len() == 1 {
        return nodes.remove(0);
    }
    // Balanced equal-weight splits stay inside the existing 0.1..=0.9 ratio
    // contract even when a large terminal can accommodate many members.
    let mid = nodes.len() / 2;
    let ratio = mid as f32 / nodes.len() as f32;
    let second = nodes.split_off(mid);
    Node::Split {
        direction,
        ratio,
        first: Box::new(sequence(direction, nodes)),
        second: Box::new(sequence(direction, second)),
    }
}
