"""Utility to write DabNotebook objects to files on disk."""

from __future__ import annotations

from pathlib import Path

from orchestra.models.dab import DabNotebook


def write_notebooks(notebooks: list[DabNotebook], output_dir: Path) -> list[Path]:
    """Write notebook content to files at output_dir/relative_path.

    Creates any intermediate directories as needed.  Each notebook is
    written with UTF-8 encoding and a trailing newline.

    Args:
        notebooks: List of DabNotebook objects to write.
        output_dir: Root directory under which notebooks are created,
            typically ``<bundle>/src/``.

    Returns:
        List of absolute paths to the created files.
    """
    created: list[Path] = []
    for nb in notebooks:
        dest = output_dir / nb.relative_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        if nb.binary_content is not None:
            dest.write_bytes(nb.binary_content)
        else:
            content = nb.content if nb.content.endswith("\n") else nb.content + "\n"
            dest.write_text(content, encoding="utf-8")
        created.append(dest.resolve())
    return created
