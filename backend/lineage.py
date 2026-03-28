"""Data lineage tracking for the execution engine (T1.3.1).

Each field in the output DataFrame carries a FieldLineage record that
describes where the field originated and every transformation it passed
through on its way to the current node.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LineageStep:
    """One transformation step applied to a field."""
    node_id: str
    command_id: str
    command_type: str
    expression: Optional[str] = None  # e.g. transform expression or agg func


@dataclass
class FieldLineage:
    """Complete lineage record for a single output field."""
    field_name: str
    origin_table: str
    origin_field: str
    transformations: List[LineageStep] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fieldName": self.field_name,
            "originTable": self.origin_table,
            "originField": self.origin_field,
            "transformations": [
                {
                    "nodeId": s.node_id,
                    "commandId": s.command_id,
                    "commandType": s.command_type,
                    "expression": s.expression,
                }
                for s in self.transformations
            ],
        }


class LineageTracker:
    """Stateful tracker that is updated as each command executes.

    The tracker is keyed by *current* field name.  When a command renames or
    derives a field the old entry is superseded by the new one.
    """

    def __init__(self) -> None:
        self._lineage: Dict[str, FieldLineage] = {}

    # ── initialise ───────────────────────────────────────────────────────────

    def init_from_source(
        self,
        source_name: str,
        fields: List[str],
        node_id: str,
        command_id: str,
    ) -> None:
        """Called once when the primary data source is first loaded."""
        for f in fields:
            self._lineage[f] = FieldLineage(
                field_name=f,
                origin_table=source_name,
                origin_field=f,
                transformations=[
                    LineageStep(
                        node_id=node_id,
                        command_id=command_id,
                        command_type="source",
                    )
                ],
            )

    # ── per-command update methods ────────────────────────────────────────────

    def record_join(
        self,
        right_table: str,
        new_fields: List[str],
        node_id: str,
        command_id: str,
    ) -> None:
        """Called after a join; registers any fields that did not exist before."""
        step = LineageStep(
            node_id=node_id,
            command_id=command_id,
            command_type="join",
        )
        for f in new_fields:
            if f not in self._lineage:
                self._lineage[f] = FieldLineage(
                    field_name=f,
                    origin_table=right_table,
                    origin_field=f,
                    transformations=[step],
                )

    def record_transform(
        self,
        output_field: str,
        expression: str,
        node_id: str,
        command_id: str,
    ) -> None:
        """Called after each mapping rule in a transform command."""
        step = LineageStep(
            node_id=node_id,
            command_id=command_id,
            command_type="transform",
            expression=expression,
        )
        existing = self._lineage.get(output_field)
        if existing:
            existing.transformations.append(step)
            existing.field_name = output_field
        else:
            self._lineage[output_field] = FieldLineage(
                field_name=output_field,
                origin_table="computed",
                origin_field=output_field,
                transformations=[step],
            )

    def record_group(
        self,
        group_fields: List[str],
        agg_aliases: List[str],
        agg_expressions: List[str],
        node_id: str,
        command_id: str,
    ) -> None:
        """Called after a group command; replaces the current field set."""
        new_lineage: Dict[str, FieldLineage] = {}
        step = LineageStep(node_id=node_id, command_id=command_id, command_type="group")

        for f in group_fields:
            existing = self._lineage.get(f)
            if existing:
                new_lineage[f] = FieldLineage(
                    field_name=f,
                    origin_table=existing.origin_table,
                    origin_field=existing.origin_field,
                    transformations=existing.transformations + [step],
                )
            else:
                new_lineage[f] = FieldLineage(
                    field_name=f,
                    origin_table="unknown",
                    origin_field=f,
                    transformations=[step],
                )

        for alias, expr in zip(agg_aliases, agg_expressions):
            new_lineage[alias] = FieldLineage(
                field_name=alias,
                origin_table="computed",
                origin_field=alias,
                transformations=[
                    LineageStep(
                        node_id=node_id,
                        command_id=command_id,
                        command_type="group",
                        expression=expr,
                    )
                ],
            )

        self._lineage = new_lineage

    def record_view(
        self,
        kept_fields: List[str],
        node_id: str,
        command_id: str,
    ) -> None:
        """Called after a view command; prunes fields not in kept_fields."""
        step = LineageStep(node_id=node_id, command_id=command_id, command_type="view")
        new_lineage: Dict[str, FieldLineage] = {}
        for f in kept_fields:
            existing = self._lineage.get(f)
            if existing:
                new_lineage[f] = FieldLineage(
                    field_name=f,
                    origin_table=existing.origin_table,
                    origin_field=existing.origin_field,
                    transformations=existing.transformations + [step],
                )
            else:
                new_lineage[f] = FieldLineage(
                    field_name=f,
                    origin_table="unknown",
                    origin_field=f,
                    transformations=[step],
                )
        self._lineage = new_lineage

    # ── serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        return {name: fl.to_dict() for name, fl in self._lineage.items()}
