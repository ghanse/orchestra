"""Motif detection and collapsing for ADF pipeline patterns."""

from orchestra.motifs.collapser import collapse_motifs
from orchestra.motifs.detector import detect_motifs

__all__ = ["detect_motifs", "collapse_motifs"]
