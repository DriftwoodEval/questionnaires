"""A progress bar pinned to the bottom row of the terminal.

Log lines scroll in the rows above it. Clients that failed are marked in red.
Outside a terminal (piped output, cron) nothing is drawn and logs print normally.
"""

import loguru
from rich import get_console
from rich.text import Text


def render_bar(total: int, completed: int, failed: set[int], width: int) -> Text:
    """Draw `width` cells for `total` clients (numbered from 1).

    A cell is red if any client it covers failed, green if every client it covers
    is done, gray otherwise. With fewer clients than cells each client spans
    several cells; with more, each cell covers several clients.
    """
    bar = Text()
    for cell in range(width):
        first = cell * total // width + 1
        last = max(first, (cell + 1) * total // width)
        if any(first <= n <= last for n in failed):
            style = "red"
        elif last <= completed:
            style = "green"
        else:
            style = "bright_black"
        bar.append("━", style=style)
    return bar


class PinnedProgressBar:
    """Context manager owning the terminal's bottom two rows.

    The bottom row is the overall client bar. The row above it is a smaller bar for
    the questionnaires of the client being processed, blank between clients.

    Entering confines scrolling to the rows above them (an ANSI scroll region), so
    everything printed through the rich console, including loguru output via
    `PinnedProgressBar.log_sink`, stays above.

    lazy: the bar height is read at draw time but the scroll region is set once, so
    resizing the terminal mid-run can leave a stray row until the run ends.
    """

    current: "PinnedProgressBar | None" = None

    def __init__(self, total: int):
        self.total = total
        self.completed = 0
        self.sent = 0
        self.failed: set[int] = set()
        self.noticed: set[int] = set()
        # Questionnaire bar for the current client; None while idle.
        self.q_total: int | None = None
        self.q_attempted = 0
        self.q_failed: set[int] = set()
        self.console = get_console()
        self.active = False

    def __enter__(self) -> "PinnedProgressBar":
        if self.console.is_terminal:
            self.active = True
            PinnedProgressBar.current = self
            height = self.console.height
            # Scroll twice so the bottom two rows are free, then confine scrolling
            # above them.
            self.console.file.write(
                f"\x1b[{height};1H\n\n\x1b[1;{height - 2}r\x1b[{height - 2};1H"
            )
            self.draw()
        return self

    def __exit__(self, *exc_info) -> None:
        if self.active:
            PinnedProgressBar.current = None
            self.active = False
            self.console.file.write(f"\x1b[r\x1b[{self.console.height};1H\n")
            self.console.file.flush()

    def advance_to(self, completed: int) -> None:
        """Move to the next client and clear the questionnaire bar."""
        self.completed = completed
        self.q_total = None
        self.draw()

    def start_questionnaires(self, total: int, done: int = 0) -> None:
        """Show the questionnaire bar for the current client.

        `done` counts questionnaires that already exist (reused links).
        """
        self.q_total = total
        self.q_attempted = done
        self.q_failed = set()
        self.draw()

    def questionnaire_attempted(self, failed: bool = False) -> None:
        self.q_attempted += 1
        if failed:
            self.q_failed.add(self.q_attempted)
        self.draw()

    def mark_sent(self) -> None:
        self.sent += 1
        self.draw()

    @property
    def skipped(self) -> int:
        """Clients with a NOTICE log that did not also fail."""
        return len(self.noticed - self.failed)

    def mark_notice(self) -> None:
        """Record a NOTICE log for the client currently being processed."""
        self.noticed.add(self.completed)
        self.draw()

    def mark_failed(self) -> None:
        """Mark the client currently being processed as failed."""
        self.failed.add(self.completed)
        self.draw()

    def draw(self) -> None:
        if not self.active:
            return
        height = self.console.height
        # Save the cursor, draw both rows, then put the cursor back.
        self.console.file.write("\x1b7")
        self._draw_row(height - 1, self._questionnaire_line())
        self._draw_row(height, self._client_line())
        self.console.file.write("\x1b8")
        self.console.file.flush()

    def _draw_row(self, row: int, line: Text | None) -> None:
        self.console.file.write(f"\x1b[{row};1H\x1b[2K")
        if line is not None:
            self.console.print(line, end="", soft_wrap=True)

    def _client_line(self) -> Text:
        errors = len(self.failed)
        summary = f" {self.completed}/{self.total} clients | {self.sent} sent | {self.skipped} skipped | "
        error_text = f"{errors} errors"
        width = max(1, self.console.width - len(summary) - len(error_text) - 1)
        line = render_bar(self.total, self.completed, self.failed, width)
        line.append(summary)
        line.append(error_text, style="red" if errors else "")
        return line

    def _questionnaire_line(self) -> Text | None:
        if self.q_total is None:
            return None
        text = f" {self.q_attempted}/{self.q_total} questionnaires generated | "
        failed_text = f"{len(self.q_failed)} failed"
        # Smaller than the client bar: at most a third of the terminal.
        width = max(
            1,
            min(
                self.console.width // 3,
                self.console.width - len(text) - len(failed_text) - 1,
            ),
        )
        line = render_bar(self.q_total, self.q_attempted, self.q_failed, width)
        line.append(text)
        line.append(failed_text, style="red" if self.q_failed else "")
        return line

    @staticmethod
    def log_sink(message: "loguru.Message") -> None:
        """Loguru sink: print through the rich console, then redraw the bar."""
        console = get_console()
        console.print(Text.from_ansi(str(message)), end="")
        bar = PinnedProgressBar.current
        if bar is None:
            return
        if message.record["level"].name == "NOTICE":
            bar.mark_notice()
        else:
            bar.draw()
