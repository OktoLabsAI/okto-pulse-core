"""F4: the detailed repair report and exclusive implementation are absent."""
import importlib.util

from okto_pulse.core.mcp import kg_power_tools, server


def test_provenance_report_is_not_a_hidden_python_fallback():
    assert importlib.util.find_spec("okto_pulse.core.kg.provenance_drift") is None
    assert not hasattr(server, "okto_pulse_kg_provenance_drift")

    class Catalog:
        def __init__(self):
            self.tools = {}

        def tool(self):
            def register(function):
                self.tools[function.__name__] = function
                return function
            return register

    def forbidden(*args, **kwargs):
        raise AssertionError("Registration must not resolve authority")

    catalog = Catalog()
    kg_power_tools.register_kg_power_tools(catalog, get_agent=forbidden, get_board_agent=forbidden)
    assert "okto_pulse_kg_provenance_drift" not in catalog.tools
    assert "okto_pulse_kg_query_cypher" in catalog.tools
    assert "okto_pulse_kg_verify_grounding" in catalog.tools
