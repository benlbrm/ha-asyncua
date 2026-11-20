"""Discover OPC UA variables and map them to Home Assistant platforms."""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from asyncua import ua

from .const import DISCOVERY_MAX_VARIABLES

if TYPE_CHECKING:
    from asyncua import Client

_LOGGER = logging.getLogger(__name__)

BATCH = 200
MAX_VISITED = 50_000

_HAS_PROPERTY = ua.NodeId(ua.ObjectIds.HasProperty)
_HIERARCHICAL = ua.NodeId(ua.ObjectIds.HierarchicalReferences)
_EU_PROPERTIES = ("EngineeringUnits", "EURange")

# Standard DataType node ids (namespace 0) that are subtypes of built-in types.
_DATATYPE_ALIASES: dict[int, ua.VariantType] = {
    26: ua.VariantType.Double,  # Number
    27: ua.VariantType.Int64,  # Integer
    28: ua.VariantType.UInt64,  # UInteger
    290: ua.VariantType.Double,  # Duration
    294: ua.VariantType.DateTime,  # UtcTime
}

NUMERIC_TYPES = frozenset(
    (
        ua.VariantType.SByte,
        ua.VariantType.Byte,
        ua.VariantType.Int16,
        ua.VariantType.UInt16,
        ua.VariantType.Int32,
        ua.VariantType.UInt32,
        ua.VariantType.Int64,
        ua.VariantType.UInt64,
        ua.VariantType.Float,
        ua.VariantType.Double,
    )
)
FLOAT_TYPES = frozenset((ua.VariantType.Float, ua.VariantType.Double))
TEXT_TYPES = frozenset((ua.VariantType.String, ua.VariantType.LocalizedText))


@dataclass(slots=True)
class DiscoveredVariable:
    """A variable found on the server and the entity it maps to."""

    nodeid: str
    name: str
    path: str
    platform: str
    vtype: str
    writable: bool
    device_id: str | None = None
    device_name: str | None = None
    unit: str | None = None
    min_value: float | None = None
    max_value: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DiscoveredVariable:
        """Deserialize from storage."""
        return cls(**{k: data.get(k) for k in cls.__slots__})  # type: ignore[attr-defined]


@dataclass(slots=True)
class DiscoveryFilter:
    """User options restricting what is discovered."""

    root: str = "i=85"
    max_depth: int = 10
    include: str | None = None
    exclude: str | None = None
    namespaces: set[int] = field(default_factory=set)
    skip_nodeids: set[str] = field(default_factory=set)
    # Writable variables become controls only when matching this regex;
    # every other variable is exposed read-only (safe default for PLCs).
    writable: str | None = None

    def is_control(self, nodeid: str, path: str) -> bool:
        """Return True if a writable variable should be exposed as a control."""
        return bool(self.writable) and bool(
            re.search(self.writable, path) or re.search(self.writable, nodeid)
        )

    def accepts(self, nodeid: str, path: str, namespace: int) -> bool:
        """Return True if the variable passes the filters."""
        if nodeid in self.skip_nodeids:
            return False
        if self.namespaces and namespace not in self.namespaces:
            return False
        if self.include and not (
            re.search(self.include, path) or re.search(self.include, nodeid)
        ):
            return False
        return not (
            self.exclude
            and (re.search(self.exclude, path) or re.search(self.exclude, nodeid))
        )


@dataclass(slots=True)
class _Pending:
    nodeid: ua.NodeId
    path: tuple[str, ...]
    device: tuple[str, str] | None
    is_variable: bool
    # Path relative to the device, used as entity name.
    rel: tuple[str, ...] = ()


_ARRAY_ELEMENT = re.compile(r"\[\d+\]$")
# EURange bounds beyond this are placeholders (e.g. +/-FLT_MAX), not real limits.
_MAX_SANE_RANGE = 1e15
# Built-in DataTypes are ns=0;i=1 (Boolean) .. ns=0;i=25 (DiagnosticInfo).
_LAST_BUILTIN_TYPE = 25


def variant_type_for(datatype: ua.NodeId) -> ua.VariantType | None:
    """Map a DataType node id to a built-in variant type (None if unsupported)."""
    if datatype.NamespaceIndex != 0 or not isinstance(datatype.Identifier, int):
        return None
    ident = datatype.Identifier
    if ident in _DATATYPE_ALIASES:
        return _DATATYPE_ALIASES[ident]
    if 1 <= ident <= _LAST_BUILTIN_TYPE:
        return ua.VariantType(ident)
    return None


def platform_for(vtype: ua.VariantType, writable: bool) -> str | None:
    """Choose the Home Assistant platform for a variable."""
    if vtype is ua.VariantType.Boolean:
        return "switch" if writable else "binary_sensor"
    if vtype in NUMERIC_TYPES:
        return "number" if writable else "sensor"
    if vtype is ua.VariantType.String:
        return "text" if writable else "sensor"
    if vtype in (ua.VariantType.LocalizedText, ua.VariantType.DateTime):
        return "sensor"
    return None


async def _browse_many(
    client: Client, nodeids: list[ua.NodeId]
) -> list[list[ua.ReferenceDescription]]:
    """Browse hierarchical forward references of several nodes, following continuation points."""
    out: list[list[ua.ReferenceDescription]] = []
    for start in range(0, len(nodeids), BATCH):
        chunk = nodeids[start : start + BATCH]
        params = ua.BrowseParameters()
        params.View = ua.ViewDescription()
        params.RequestedMaxReferencesPerNode = 0
        for nodeid in chunk:
            desc = ua.BrowseDescription()
            desc.NodeId = nodeid
            desc.BrowseDirection = ua.BrowseDirection.Forward
            desc.ReferenceTypeId = _HIERARCHICAL
            desc.IncludeSubtypes = True
            desc.NodeClassMask = ua.NodeClass.Object | ua.NodeClass.Variable
            desc.ResultMask = ua.BrowseResultMask.All
            params.NodesToBrowse.append(desc)
        results = await client.uaclient.browse(params)
        for result in results:
            refs = list(result.References or [])
            continuation = result.ContinuationPoint
            while continuation:
                next_params = ua.BrowseNextParameters()
                next_params.ReleaseContinuationPoints = False
                next_params.ContinuationPoints = [continuation]
                next_result = (await client.uaclient.browse_next(next_params))[0]
                refs.extend(next_result.References or [])
                continuation = next_result.ContinuationPoint
            out.append(refs)
    return out


async def _read_many(
    client: Client, items: list[tuple[ua.NodeId, ua.AttributeIds]]
) -> list[ua.DataValue]:
    out: list[ua.DataValue] = []
    for start in range(0, len(items), BATCH * 2):
        params = ua.ReadParameters()
        for nodeid, attr in items[start : start + BATCH * 2]:
            rv = ua.ReadValueId()
            rv.NodeId = nodeid
            rv.AttributeId = attr
            params.NodesToRead.append(rv)
        out.extend(await client.uaclient.read(params))
    return out


def _value(data_value: ua.DataValue) -> Any:
    if data_value.StatusCode is not None and not data_value.StatusCode.is_good():
        return None
    return data_value.Value.Value if data_value.Value is not None else None


async def async_discover(  # noqa: C901
    client: Client, flt: DiscoveryFilter
) -> tuple[list[DiscoveredVariable], bool]:
    """
    Browse the address space under flt.root.

    Returns (variables, truncated). Objects become devices (nearest object
    ancestor of each variable), variables become entities.
    """
    root = ua.NodeId.from_string(flt.root)
    level: list[_Pending] = [_Pending(root, (), None, False)]
    visited: set[ua.NodeId] = {root}
    candidates: list[_Pending] = []
    eu_props: dict[ua.NodeId, dict[str, ua.NodeId]] = {}
    truncated = False

    for _depth in range(flt.max_depth):
        if not level:
            break
        refs_per_node = await _browse_many(client, [p.nodeid for p in level])
        next_level: list[_Pending] = []
        for parent, refs in zip(level, refs_per_node, strict=True):
            for ref in refs:
                child = ua.NodeId(ref.NodeId.Identifier, ref.NodeId.NamespaceIndex)
                name = ref.BrowseName.Name or ""
                if ref.ReferenceTypeId == _HAS_PROPERTY:
                    if parent.is_variable and name in _EU_PROPERTIES:
                        eu_props.setdefault(parent.nodeid, {})[name] = child
                    continue
                if child in visited or child.NamespaceIndex == 0:
                    continue
                visited.add(child)
                if len(visited) > MAX_VISITED:
                    truncated = True
                    break
                display = (ref.DisplayName.Text if ref.DisplayName else None) or name
                path = (*parent.path, display)
                if ref.NodeClass == ua.NodeClass.Object:
                    next_level.append(
                        _Pending(child, path, (child.to_string(), display), False)
                    )
                elif ref.NodeClass == ua.NodeClass.Variable:
                    pending = _Pending(
                        child, path, parent.device, True, (*parent.rel, display)
                    )
                    candidates.append(pending)
                    if _ARRAY_ELEMENT.search(display):
                        # Element of an array of structures: its members form a device.
                        next_level.append(
                            _Pending(
                                child, path, (child.to_string(), display), True, ()
                            )
                        )
                    else:
                        next_level.append(pending)
        level = next_level

    # Read the attributes needed to classify each candidate.
    attrs = (
        ua.AttributeIds.DataType,
        ua.AttributeIds.ValueRank,
        ua.AttributeIds.UserAccessLevel,
    )
    reads = [(c.nodeid, attr) for c in candidates for attr in attrs]
    values = await _read_many(client, reads) if reads else []

    variables: list[DiscoveredVariable] = []
    with_eu: list[tuple[DiscoveredVariable, ua.NodeId]] = []
    for index, cand in enumerate(candidates):
        datatype, value_rank, access = (
            _value(v) for v in values[index * 3 : index * 3 + 3]
        )
        if datatype is None or access is None:
            continue
        if value_rank not in (None, -1):  # arrays are not supported
            continue
        if not access & ua.AccessLevel.CurrentRead.mask:
            continue
        vtype = variant_type_for(datatype)
        if vtype is None:
            continue
        nodeid = cand.nodeid.to_string()
        path = "/".join(cand.path)
        if not flt.accepts(nodeid, path, cand.nodeid.NamespaceIndex):
            continue
        writable = bool(access & ua.AccessLevel.CurrentWrite.mask) and flt.is_control(
            nodeid, path
        )
        platform = platform_for(vtype, writable)
        if platform is None:
            continue
        if len(variables) >= DISCOVERY_MAX_VARIABLES:
            truncated = True
            break
        var = DiscoveredVariable(
            nodeid=nodeid,
            name=".".join(cand.rel) or cand.path[-1],
            path=path,
            platform=platform,
            vtype=vtype.name,
            writable=writable,
            device_id=cand.device[0] if cand.device else None,
            device_name=cand.device[1] if cand.device else None,
        )
        variables.append(var)
        # EU property kind is re-derived from the value type when read below.
        with_eu.extend(
            (var, prop_node) for prop_node in eu_props.get(cand.nodeid, {}).values()
        )

    if with_eu:
        eu_values = await _read_many(
            client, [(n, ua.AttributeIds.Value) for _, n in with_eu]
        )
        for (var, _), data_value in zip(with_eu, eu_values, strict=True):
            value = _value(data_value)
            if isinstance(value, ua.EUInformation):
                text = value.DisplayName.Text if value.DisplayName else None
                var.unit = text or None
            elif isinstance(value, ua.Range):
                low, high = float(value.Low), float(value.High)
                if low < high and max(abs(low), abs(high)) < _MAX_SANE_RANGE:
                    var.min_value, var.max_value = low, high

    if truncated:
        _LOGGER.warning(
            "OPC UA discovery truncated (more than %d variables): restrict it in the "
            "integration options (Configure), e.g. keep one namespace or set a root node",
            DISCOVERY_MAX_VARIABLES,
        )
    return variables, truncated
