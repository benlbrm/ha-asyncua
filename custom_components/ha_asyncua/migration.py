"""
Migration from the former ``ha-asyncua`` domain (before 0.3.0).

Home Assistant only accepts domains made of lowercase letters, digits and
underscores (brand images, hassfest, HACS), so the domain became ``ha_asyncua``.
This module keeps existing installations working:

* entity and device registry entries of the old domain are moved to the new one
  (same entity ids, names, customisations and history);
* UI config entries of the old domain are re-created under the new domain, and
  the old entries are removed once their entities have been adopted;
* YAML written with the old domain keeps working, with a repair issue asking to
  rename it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import SOURCE_IMPORT
from homeassistant.core import callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store

from .const import DOMAIN, OLD_DOMAIN

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

SOURCE_MIGRATE = "migrate"
ISSUE_OLD_YAML = "old_yaml_domain"
_OLD_PREFIX = f"{OLD_DOMAIN}."


def new_unique_id(unique_id: str) -> str:
    """Map a unique id built with the old domain to the new domain."""
    if unique_id.startswith(_OLD_PREFIX):
        return f"{DOMAIN}.{unique_id[len(_OLD_PREFIX) :]}"
    return unique_id


@callback
def async_migrate_registries(hass: HomeAssistant) -> None:
    """Move entities and devices of the old domain (before any platform loads)."""
    ent_reg = er.async_get(hass)
    for entity in [e for e in ent_reg.entities.values() if e.platform == OLD_DOMAIN]:
        unique_id = new_unique_id(entity.unique_id)
        if ent_reg.async_get_entity_id(entity.domain, DOMAIN, unique_id):
            # Already migrated (or re-created meanwhile): drop the stale copy.
            ent_reg.async_remove(entity.entity_id)
            continue
        kwargs: dict[str, Any] = {"new_unique_id": unique_id}
        if entity.config_entry_id is not None:
            # Still linked to the old entry until the new entry adopts it.
            kwargs["new_config_entry_id"] = entity.config_entry_id
        ent_reg.async_update_entity_platform(entity.entity_id, DOMAIN, **kwargs)
        _LOGGER.info("Migrated %s to the %s domain", entity.entity_id, DOMAIN)

    dev_reg = dr.async_get(hass)
    for device in list(dev_reg.devices.values()):
        old = {i for i in device.identifiers if i[0] == OLD_DOMAIN}
        if not old:
            continue
        new = {(DOMAIN, i[1]) for i in old}
        if any(dev_reg.async_get_device(identifiers={i}) for i in new):
            continue
        dev_reg.async_update_device(
            device.id, new_identifiers=(device.identifiers - old) | new
        )


@callback
def async_migrate_config_entries(hass: HomeAssistant) -> None:
    """Re-create UI entries of the old domain (YAML ones are re-imported)."""
    for old in hass.config_entries.async_entries(OLD_DOMAIN):
        if old.source == SOURCE_IMPORT or old.unique_id is None:
            continue
        if hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, old.unique_id):
            continue
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN,
                context={"source": SOURCE_MIGRATE},
                data={
                    "title": old.title,
                    "unique_id": old.unique_id,
                    "data": dict(old.data),
                    "options": dict(old.options),
                },
            )
        )


async def async_adopt_old_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Take over entities and devices of the old entry with the same hub, then remove it."""
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    for old in hass.config_entries.async_entries(OLD_DOMAIN):
        if old.unique_id != entry.unique_id:
            continue
        for entity in er.async_entries_for_config_entry(ent_reg, old.entry_id):
            if entity.platform == DOMAIN:
                ent_reg.async_update_entity(
                    entity.entity_id, config_entry_id=entry.entry_id
                )
        for device in dr.async_entries_for_config_entry(dev_reg, old.entry_id):
            dev_reg.async_update_device(
                device.id,
                add_config_entry_id=entry.entry_id,
                remove_config_entry_id=old.entry_id,
            )
        await Store(hass, 1, f"{OLD_DOMAIN}.{old.entry_id}.discovery").async_remove()
        _LOGGER.info("Migrated OPC UA hub %s to the %s domain", old.title, DOMAIN)
        hass.async_create_task(hass.config_entries.async_remove(old.entry_id))


@callback
def async_report_old_yaml(hass: HomeAssistant, keys: list[str]) -> None:
    """Raise (or clear) the repair issue about YAML using the old domain."""
    if not keys:
        ir.async_delete_issue(hass, DOMAIN, ISSUE_OLD_YAML)
        return
    _LOGGER.warning(
        "configuration.yaml uses the former domain %r (%s): replace it with %r",
        OLD_DOMAIN,
        ", ".join(keys),
        DOMAIN,
    )
    ir.async_create_issue(
        hass,
        DOMAIN,
        ISSUE_OLD_YAML,
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_OLD_YAML,
        translation_placeholders={
            "old": OLD_DOMAIN,
            "new": DOMAIN,
            "keys": ", ".join(keys),
        },
    )
