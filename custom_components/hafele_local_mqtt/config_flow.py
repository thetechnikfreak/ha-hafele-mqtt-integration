"""Config flow for Hafele Local MQTT integration."""
from __future__ import annotations

import logging
import secrets
import string
import uuid
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.auth.const import GROUP_ID_USER
from homeassistant.components import person
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
import homeassistant.helpers.config_validation as cv

from .const import (
    CONF_ENABLE_GROUPS,
    CONF_ENABLE_SCENES,
    CONF_MQTT_BROKER,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_POLLING_INTERVAL,
    CONF_POLLING_MODE,
    CONF_POLLING_TIMEOUT,
    CONF_TOPIC_PREFIX,
    CONF_USE_HA_MQTT,
    DEFAULT_MQTT_PORT,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_POLLING_MODE,
    DEFAULT_POLLING_TIMEOUT,
    DEFAULT_TOPIC_PREFIX,
    DOMAIN,
    POLLING_MODE_NORMAL,
    POLLING_MODE_ROTATIONAL,
)

_LOGGER = logging.getLogger(__name__)

def generate_password(length: int = 8) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits + "!@#$%&*-_"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


async def validate_manual_input(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Validate manual MQTT configuration."""
    try:
        import paho.mqtt.client as mqtt_client
        
        client = mqtt_client.Client()
        
        if data.get(CONF_MQTT_USERNAME) and data.get(CONF_MQTT_PASSWORD):
            client.username_pw_set(data[CONF_MQTT_USERNAME], data[CONF_MQTT_PASSWORD])
        
        await hass.async_add_executor_job(
            client.connect, data[CONF_MQTT_BROKER], data[CONF_MQTT_PORT], 10
        )
        await hass.async_add_executor_job(client.disconnect)
        
        return {"title": f"Häfele Mesh ({data[CONF_MQTT_BROKER]})"}
    except Exception as err:
        _LOGGER.error("Could not connect to MQTT broker: %s", err)
        raise CannotConnect from err


async def create_mosquitto_user(hass: HomeAssistant, base_username: str, password: str) -> tuple[bool, str]:
    """
    Creates a standard HA user with a random suffix for MQTT and restarts Mosquitto.
    Returns a tuple: (Success_Boolean, Generated_Username)
    """
    random_code = uuid.uuid4().hex[:6]
    actual_username = f"{base_username}_{random_code}"
    
    try:
       
        provider = next(
            (prv for prv in hass.auth.auth_providers if prv.type == "homeassistant"), 
            None
        )
        if not provider:
            _LOGGER.error("Default auth provider not found. Cannot create MQTT user.")
            return False, ""

        await provider.async_initialize()
        
       
        await provider.async_add_auth(actual_username, password)

        display_name = f"MQTT Client ({actual_username})"
        user = await hass.auth.async_create_user(display_name, group_ids=[GROUP_ID_USER])

        credentials = await provider.async_get_or_create_credentials({"username": actual_username})
        await hass.auth.async_link_user(user, credentials)

      
        if "person" in hass.config.components:
            await person.async_create_person(hass, display_name, user_id=user.id)
            
        _LOGGER.info(f"Successfully created Mosquitto/HA user '{actual_username}'.")
        
 
        try:
            _LOGGER.info("Sending restart command to Mosquitto Add-on...")
           
            await hass.services.async_call(
                domain="hassio",
                service="addon_restart",
                service_data={"addon": "core_mosquitto"},
                blocking=False 
            )
            _LOGGER.info("Mosquitto Add-on restart initiated successfully.")
        except Exception as addon_err:
        
            _LOGGER.warning(f"Could not automatically restart Mosquitto add-on: {addon_err}")

        return True, actual_username

    except Exception as err:
        _LOGGER.error(f"Could not create MQTT user '{actual_username}': {err}")
        return False, ""
class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Häfele Mesh."""

    VERSION = 1

    def __init__(self):
        """Initialize the config flow."""
        self._setup_type = None
        self._generated_username = None
        self._generated_password = None
        self._mqtt_config = {}

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial setup type selection."""
        if user_input is not None:
            self._setup_type = user_input["setup_type"]
            
            if self._setup_type == "automatic":
                return await self.async_step_automatic()
            else:
                return await self.async_step_manual()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required("setup_type", default="automatic"): vol.In({
                    "automatic": "Automatic (Mosquitto Add-on)",
                    "manual": "Manual MQTT Configuration"
                })
            }),
        )

    async def async_step_automatic(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle automatic setup with Mosquitto."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            username = "haefele_mesh"
            password = generate_password()
            
            user_created, actual_username = await create_mosquitto_user(self.hass, username, password)
            
            if not user_created:
                _LOGGER.warning("Could not auto-create user, showing manual instructions")
                return await self.async_step_manual()
            
            config_data = {
                CONF_MQTT_BROKER: "localhost",
                CONF_MQTT_PORT: 1883,
                CONF_MQTT_USERNAME: actual_username, 
                CONF_MQTT_PASSWORD: password,
                CONF_TOPIC_PREFIX: user_input.get(CONF_TOPIC_PREFIX, "Mesh"),
            }
            
            self._generated_username = actual_username 
            self._generated_password = password
            self._mqtt_config = config_data
            
            return await self.async_step_show_credentials()

        return self.async_show_form(
            step_id="automatic",
            data_schema=vol.Schema({
                vol.Required(CONF_TOPIC_PREFIX, default="Mesh"): str,
            }),
            description_placeholders={
                "info": "A secure user will be created automatically in Mosquitto."
            },
        )

    async def async_step_show_credentials(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show generated credentials to user."""
        if user_input is not None:
            await self.async_set_unique_id(
                f"localhost_{self._mqtt_config[CONF_TOPIC_PREFIX]}"
            )
            self._abort_if_unique_id_configured()
            
            return self.async_create_entry(
                title="Häfele Mesh (Mosquitto)",
                data=self._mqtt_config,
            )

        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            local_ip = "YOUR_HOME_ASSISTANT_IP"

        return self.async_show_form(
            step_id="show_credentials",
            data_schema=vol.Schema({}),
            description_placeholders={
                "broker_url": f"mqtt://{local_ip}:1883",
                "username": self._generated_username,
                "password": self._generated_password,
                "topic": self._mqtt_config[CONF_TOPIC_PREFIX],
                "instructions": (
                    "⚠️ IMPORTANT: Save these credentials now!\n\n"
                    "Enter these details in your Häfele Mesh Connect app:\n\n"
                    f"• MQTT Broker: mqtt://{local_ip}:1883\n"
                    f"• Username: {self._generated_username}\n"
                    f"• Password: {self._generated_password}\n"
                    f"• Topic: {self._mqtt_config[CONF_TOPIC_PREFIX]}\n\n"
                    "Click Submit when you have saved these credentials."
                )
            },
        )

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle manual MQTT configuration."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            try:
                info = await validate_manual_input(self.hass, user_input)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(
                    f"{user_input[CONF_MQTT_BROKER]}_{user_input[CONF_TOPIC_PREFIX]}"
                )
                self._abort_if_unique_id_configured()
                
                return self.async_create_entry(title=info["title"], data=user_input)

        return self.async_show_form(
            step_id="manual",
            data_schema=vol.Schema({
                vol.Required(CONF_MQTT_BROKER, default="localhost"): str,
                vol.Required(CONF_MQTT_PORT, default=1883): cv.port,
                vol.Optional(CONF_MQTT_USERNAME): str,
                vol.Optional(CONF_MQTT_PASSWORD): str,
                vol.Required(CONF_TOPIC_PREFIX, default="Mesh"): str,
                vol.Optional(
                    CONF_POLLING_INTERVAL, default=DEFAULT_POLLING_INTERVAL
                ): vol.All(vol.Coerce(int), vol.Range(min=2, max=300)),
                vol.Optional(
                    CONF_POLLING_TIMEOUT, default=DEFAULT_POLLING_TIMEOUT
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=30)),
                vol.Optional(
                    CONF_POLLING_MODE, default=DEFAULT_POLLING_MODE
                ): vol.In([POLLING_MODE_NORMAL, POLLING_MODE_ROTATIONAL]),
                vol.Optional(CONF_ENABLE_GROUPS, default=True): bool,
                vol.Optional(CONF_ENABLE_SCENES, default=True): bool,                
            }),
            errors=errors,
        )


class CannotConnect(Exception):
    """Error to indicate we cannot connect."""
