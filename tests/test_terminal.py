from __future__ import annotations

import unittest

from kctl_pkg import terminal


class _TTYStream:
    def __init__(self, is_tty: bool) -> None:
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


class TerminalTests(unittest.TestCase):
    def setUp(self) -> None:
        self._color_enabled = terminal.COLOR_ENABLED
        terminal.set_color_enabled(False)

    def tearDown(self) -> None:
        terminal.set_color_enabled(self._color_enabled)

    def test_supports_color_requires_global_flag_and_tty(self) -> None:
        stream = _TTYStream(True)
        self.assertFalse(terminal.supports_color(stream))

        terminal.set_color_enabled(True)
        self.assertTrue(terminal.supports_color(stream))
        self.assertFalse(terminal.supports_color(_TTYStream(False)))

    def test_style_text_adds_ansi_codes_when_enabled(self) -> None:
        terminal.set_color_enabled(True)

        styled = terminal.style_text(
            "hello",
            stream=_TTYStream(True),
            color=terminal.ANSI_GREEN,
            bold=True,
            dim=True,
        )

        self.assertTrue(styled.startswith(terminal.ANSI_BOLD + terminal.ANSI_DIM + terminal.ANSI_GREEN))
        self.assertTrue(styled.endswith(terminal.ANSI_RESET))

    def test_style_status_text_maps_common_statuses(self) -> None:
        terminal.set_color_enabled(True)
        stream = _TTYStream(True)

        self.assertIn(terminal.ANSI_GREEN, terminal.style_status_text("ok", "success", stream=stream))
        self.assertIn(terminal.ANSI_YELLOW, terminal.style_status_text("warn", "paused", stream=stream))
        self.assertIn(terminal.ANSI_RED, terminal.style_status_text("bad", "failed", stream=stream))
        self.assertEqual(terminal.style_status_text("plain", "unknown", stream=_TTYStream(False)), "plain")

    def test_is_command_like_line_detects_shell_commands(self) -> None:
        self.assertTrue(terminal.is_command_like_line("git status"))
        self.assertTrue(terminal.is_command_like_line("`python3 -m unittest`"))
        self.assertFalse(terminal.is_command_like_line("This is a sentence."))

    def test_is_important_output_line_detects_errors(self) -> None:
        self.assertTrue(terminal.is_important_output_line("error: broken"))
        self.assertTrue(terminal.is_important_output_line("Permission denied"))
        self.assertFalse(terminal.is_important_output_line("all good"))

    def test_looks_like_code_or_file_dump_detects_code_shapes(self) -> None:
        self.assertTrue(terminal.looks_like_code_or_file_dump("const value = 1;"))
        self.assertTrue(terminal.looks_like_code_or_file_dump("<Widget prop='x'>"))
        self.assertTrue(terminal.looks_like_code_or_file_dump("src/app.py:12: print('hi')"))
        self.assertTrue(terminal.looks_like_code_or_file_dump("1 const value = 1;"))
        self.assertFalse(terminal.looks_like_code_or_file_dump("This is a plain sentence."))

    def test_looks_like_natural_language_line_accepts_result_and_prose_lines(self) -> None:
        self.assertTrue(terminal.looks_like_natural_language_line("Verification passed with exit code 0."))
        self.assertTrue(terminal.looks_like_natural_language_line("- Added a regression test for the route."))
        self.assertFalse(terminal.looks_like_natural_language_line("git status"))
        self.assertFalse(terminal.looks_like_natural_language_line("src/app.py:12: print('hi')"))
        self.assertFalse(terminal.looks_like_natural_language_line("- git status"))

    def test_should_display_codex_line_filters_preamble_and_tokens(self) -> None:
        self.assertFalse(terminal.should_display_codex_line("OpenAI Codex v1"))
        self.assertFalse(terminal.should_display_codex_line("token usage input 10 output 20"))
        self.assertFalse(terminal.should_display_codex_line("Current step id: inspect"))
        self.assertTrue(terminal.should_display_codex_line("warning: test failed"))
        self.assertTrue(terminal.should_display_codex_line("codex: Updated the session page layout."))

    def test_should_display_claude_line_filters_prompt_sections(self) -> None:
        self.assertFalse(terminal.should_display_claude_line("Constraints:"))
        self.assertFalse(terminal.should_display_claude_line("claude: token usage input 10 output 20"))
        self.assertTrue(terminal.should_display_claude_line("claude: Added coverage for review helpers."))
        self.assertTrue(terminal.should_display_claude_line("fatal: repository not found"))

    def test_is_meaningful_summary_line_filters_noise(self) -> None:
        self.assertFalse(terminal.is_meaningful_summary_line("OpenAI Codex v1"))
        self.assertFalse(terminal.is_meaningful_summary_line("tests:"))
        self.assertFalse(terminal.is_meaningful_summary_line("git status"))
        self.assertTrue(terminal.is_meaningful_summary_line("Added review coverage and reran the suite."))


if __name__ == "__main__":
    unittest.main()
