use crate::api::schema::{
    Method, PaneTarget, Request, TerminalCloseIfParams, TerminalCondition, TerminalInputIfParams,
    TerminalRenameIfParams,
};
use clap::{Arg, Command};

pub(super) fn commands() -> Vec<Command> {
    let condition = || {
        Arg::new("condition").long("condition").value_name("PATH").required(true).help("JSON condition file from terminal condition; never falls back to an unguarded operation")
    };
    vec![
        Command::new("condition")
            .about("Read exact live terminal runtime identity; excludes agent-process identity")
            .arg(Arg::new("pane").required(true).value_name("PANE_ID")),
        Command::new("rename-if")
            .about("Rename only the matching terminal runtime")
            .arg(condition())
            .arg(Arg::new("label").long("label").value_name("LABEL")),
        Command::new("close-if")
            .about("Close only the matching terminal runtime, preserving close protections")
            .arg(condition()),
        Command::new("input-if")
            .about("Reject raw PTY input that cannot isolate an original reader")
            .arg(condition())
            .arg(
                Arg::new("text")
                    .long("text")
                    .value_name("TEXT")
                    .required(true),
            ),
    ]
}

pub(super) fn run(args: &[String]) -> std::io::Result<i32> {
    let matches = match Command::new("terminal")
        .bin_name("herdr terminal")
        .subcommands(commands())
        .no_binary_name(true)
        .try_get_matches_from(args)
    {
        Ok(matches) => matches,
        Err(error) => return super::print_clap_error(&error),
    };
    let Some((name, matches)) = matches.subcommand() else {
        return Ok(2);
    };
    let method = if name == "condition" {
        let Some(pane) = matches.get_one::<String>("pane") else {
            return Ok(2);
        };
        Method::TerminalCondition(PaneTarget {
            pane_id: super::normalize_pane_id(pane),
        })
    } else {
        let Some(path) = matches.get_one::<String>("condition") else {
            return Ok(2);
        };
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        // Accept the exact condition or the JSON response produced by this CLI.
        let value = value.pointer("/result/condition").unwrap_or(&value).clone();
        let condition: TerminalCondition = serde_json::from_value(value)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        match name {
            "rename-if" => Method::TerminalRenameIf(TerminalRenameIfParams {
                condition,
                label: matches.get_one::<String>("label").cloned(),
            }),
            "close-if" => Method::TerminalCloseIf(TerminalCloseIfParams { condition }),
            "input-if" => Method::TerminalInputIf(TerminalInputIfParams {
                condition,
                text: matches
                    .get_one::<String>("text")
                    .cloned()
                    .unwrap_or_default(),
            }),
            _ => return Ok(2),
        }
    };
    let request = Request {
        id: format!("cli:terminal:{name}"),
        method,
    };
    // A missing method must remain an error; never retry legacy pane operations.
    super::print_response(&super::send_request(&request)?)
}
