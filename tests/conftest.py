"""Fixtures: an embedded asyncua server and integration helpers."""

from __future__ import annotations

import importlib
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest
from asyncua import Server, ua

import custom_components  # noqa: F401 - bind the package to this repo before HA fixtures

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from asyncua.common.node import Node

DOMAIN = "ha_asyncua"


def integration_module(name: str = "") -> Any:
    """Import a module of the integration (its package name contains a hyphen)."""
    return importlib.import_module(
        f"custom_components.{DOMAIN}{'.' + name if name else ''}"
    )


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable loading custom_components."""


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class OpcServer:
    """Handle on the embedded server and its test nodes."""

    server: Server
    url: str
    ns: int
    nodes: dict[str, Node]

    def nodeid(self, key: str) -> str:
        return self.nodes[key].nodeid.to_string()

    async def read(self, key: str) -> Any:
        return await self.nodes[key].read_value()

    async def write(self, key: str, value: Any, vtype: ua.VariantType) -> None:
        await self.nodes[key].write_value(ua.Variant(value, vtype))


@pytest.fixture
async def opc_server(socket_enabled: None) -> AsyncGenerator[OpcServer]:
    """Start an OPC UA server exposing a small PLC-like address space."""
    port = _free_port()
    url = f"opc.tcp://127.0.0.1:{port}/"
    server = Server()
    await server.init()
    server.set_endpoint(url)
    ns = await server.register_namespace("urn:test:plc")
    objects = server.nodes.objects

    machine = await objects.add_object(
        ua.NodeId("Machine", ns), ua.QualifiedName("Machine", ns)
    )
    nodes: dict[str, Node] = {}

    async def var(
        parent: Node, name: str, value: Any, vtype: ua.VariantType, *, rw: bool
    ) -> Node:
        node = await parent.add_variable(
            ua.NodeId(f"Machine.{name}", ns),
            ua.QualifiedName(name, ns),
            ua.Variant(value, vtype),
        )
        if rw:
            await node.set_writable()
        nodes[name] = node
        return node

    await var(machine, "Running", True, ua.VariantType.Boolean, rw=False)
    await var(machine, "cmdStart", False, ua.VariantType.Boolean, rw=True)
    await var(machine, "Setpoint", 10, ua.VariantType.Int16, rw=True)
    temperature = await var(
        machine, "Temperature", 21.5, ua.VariantType.Double, rw=False
    )
    await var(machine, "Label", "hello", ua.VariantType.String, rw=True)
    await var(machine, "Position", 40, ua.VariantType.Int32, rw=False)

    # EngineeringUnits property on Temperature.
    eu = ua.EUInformation()
    eu.DisplayName = ua.LocalizedText("°C")
    await temperature.add_property(
        ua.NodeId("Machine.Temperature.EU", ns),
        ua.QualifiedName("EngineeringUnits", 0),
        ua.Variant(eu, ua.VariantType.ExtensionObject),
    )
    # EURange on Setpoint.
    rng = ua.Range(Low=0.0, High=100.0)
    await nodes["Setpoint"].add_property(
        ua.NodeId("Machine.Setpoint.Range", ns),
        ua.QualifiedName("EURange", 0),
        ua.Variant(rng, ua.VariantType.ExtensionObject),
    )

    # Array of structures: Motors[0].Speed / Motors[0].On -> device "Motors[0]".
    motors = await machine.add_variable(
        ua.NodeId("Machine.Motors", ns),
        ua.QualifiedName("Motors", ns),
        0,
        datatype=ua.NodeId(ua.ObjectIds.Structure),
    )
    for index in range(2):
        element = await motors.add_variable(
            ua.NodeId(f"Machine.Motors[{index}]", ns),
            ua.QualifiedName(f"Motors[{index}]", ns),
            0,
            datatype=ua.NodeId(ua.ObjectIds.Structure),
        )
        for name, value, vtype in (
            ("Speed", 1500.0, ua.VariantType.Float),
            ("On", False, ua.VariantType.Boolean),
        ):
            node = await element.add_variable(
                ua.NodeId(f"Machine.Motors[{index}].{name}", ns),
                ua.QualifiedName(name, ns),
                ua.Variant(value, vtype),
            )
            await node.set_writable()
            nodes[f"Motors[{index}].{name}"] = node

    async with server:
        yield OpcServer(server=server, url=url, ns=ns, nodes=nodes)
