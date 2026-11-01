# Existing MCP implementations checked

Checked on 2026-11-02. This is a source/documentation comparison, not a measured
hardware benchmark of every server.

| Project | Relevant strengths | Reason for the current choice |
| --- | --- | --- |
| [synman/bambu-mcp](https://github.com/synman/bambu-mcp) | Existing LAN tools, persistent sessions, files, filament and camera support | This is our upstream, not a replacement written from scratch. Its latest main is the base commit of this fork. Our changes address the observed FTPS/MQTT failure and add the public-boundary E2E gate. |
| [DMontgomery40/bambu-printer-mcp](https://github.com/DMontgomery40/bambu-printer-mcp) | Broad slicer/Blender integration, camera snapshots and fresh-report safety checks | A substantial alternative. Its inspected MCP tool list does not expose an equivalent standalone external-holder load operation; adopting it would change the current PETG troubleshooting workflow. Its slicer capabilities are broader than this fork. |
| [griches/bambu-mcp](https://github.com/griches/bambu-mcp) | Small LAN/fleet implementation with print, file and AMS operations | In the inspected MQTT client, `close` clears `connected`, but subsequent `connect` callbacks return immediately after the initial promise is settled. That leaves its own connected flag false after a reconnect. Camera tools control recording/timelapse rather than returning an image. |
| [schwarztim/bambu-mcp](https://github.com/schwarztim/bambu-mcp) | LAN MQTT/FTPS and command signing support | The inspected MQTT client has the same settled-connect/cleared-connected pattern. This is a source finding, not a live fault-campaign result. |
| [bradsjm/bambu-mqtt-proxy](https://github.com/bradsjm/bambu-mqtt-proxy) | Shared MQTT connections, bounded camera snapshots and MCP monitoring | Its core control tools expose pause/resume/stop, light and speed; they do not replace the current upload/start/filament workflow. |

Decision: retain the existing synman implementation and publish the recovery work
as a fork. The justification is the concrete repaired failure and E2E evidence,
not a claim to be the best MCP for every Bambu model or slicing task. Re-evaluate
an upstream replacement against these same public workflows before migration.
