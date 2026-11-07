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
    """Context manager owning the terminal's bottom row.

    Entering confines scrolling to the rows above the bar (an ANSI scroll region),
    so everything printed through the rich console, including loguru output via
    `PinnedProgressBar.log_sink`, stays above it.

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
        self.console = get_console()
        self.active = False

    def __enter__(self) -> "PinnedProgressBar":
        if self.console.is_terminal:
            self.active = True
            PinnedProgressBar.current = self
            height = self.console.height
            # Scroll once so the bottom row is free, then confine scrolling above it.
            self.console.file.write(
                f"\x1b[{height};1H\n\x1b[1;{height - 1}r\x1b[{height - 1};1H"
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
        self.completed = completed
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
        errors = len(self.failed)
        summary = f" {self.completed}/{self.total} clients | {self.sent} sent | {self.skipped} skipped | "
        error_text = f"{errors} errors"
        width = max(1, self.console.width - len(summary) - len(error_text) - 1)
        line = render_bar(self.total, self.completed, self.failed, width)
        line.append(summary)
        line.append(error_text, style="red" if errors else "")
        # Save the cursor, draw on the bottom row, then put the cursor back.
        self.console.file.write(f"\x1b7\x1b[{self.console.height};1H\x1b[2K")
        self.console.print(line, end="", soft_wrap=True)
        self.console.file.write("\x1b8")
        self.console.file.flush()

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
