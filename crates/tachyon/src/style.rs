#![forbid(unsafe_code)]

use std::fmt::Display;
use std::sync::OnceLock;

use anstyle::{AnsiColor, Color, Style};

pub struct Palette {
    pub good: Style,
    pub bad: Style,
    pub warn: Style,
    pub idle: Style,
    pub accent: Style,
    pub dim: Style,
}

const fn c(color: AnsiColor) -> Color {
    Color::Ansi(color)
}

static PALETTE: OnceLock<Palette> = OnceLock::new();

pub fn palette() -> &'static Palette {
    PALETTE.get_or_init(|| Palette {
        good: Style::new().fg_color(Some(c(AnsiColor::BrightCyan))).bold(),
        bad: Style::new().fg_color(Some(c(AnsiColor::Red))).bold(),
        warn: Style::new().fg_color(Some(c(AnsiColor::Yellow))).bold(),
        idle: Style::new()
            .fg_color(Some(c(AnsiColor::BrightBlack)))
            .bold(),
        accent: Style::new()
            .fg_color(Some(c(AnsiColor::BrightGreen)))
            .bold(),
        dim: Style::new().fg_color(Some(c(AnsiColor::BrightBlack))),
    })
}

pub fn render<S: Display>(style: &Style, text: S) -> String {
    format!("{style}{text}{style:#}")
}

pub fn colored_glyph(active: bool) -> String {
    if active {
        render(&palette().good, "●")
    } else {
        render(&palette().idle, "○")
    }
}

pub fn status_word(state: &str) -> String {
    let p = palette();
    let (color, word) = match state {
        "active" | "completed" => (&p.good, state),
        "failed" => (&p.bad, state),
        "interrupted" | "terminated" => (&p.warn, state),
        "starting" => (&p.warn, state),
        _ => (&p.idle, state),
    };
    render(color, word)
}
