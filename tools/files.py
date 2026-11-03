"""
tools/files.py — File management tools for Bambu Lab printers.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

from session_manager import session_manager
from reliability import SdCardBusy


def _no_printer(name: str) -> dict:
    return {"error": f"Printer '{name}' not connected"}


def _permission_denied(consequence: str) -> str:
    return f"Error: user_permission must be True to perform this action. {consequence}"


def _to_dict(o):
    """Recursively convert dataclasses/enums into plain JSON-serializable values."""
    import dataclasses
    from enum import Enum

    if isinstance(o, Enum):
        return o.name
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return {f.name: _to_dict(getattr(o, f.name)) for f in dataclasses.fields(o)}
    if isinstance(o, dict):
        return {k: _to_dict(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_dict(v) for v in o]
    return o


def _serialize_project_info(info, include_images: bool = False) -> dict:
    """
    Convert a single ProjectInfo (dataclass or dict) into the JSON-safe shape
    returned by get_project_info(), omitting the large topimg/thumbnail image
    fields unless include_images=True. Shared by get_project_info() and
    get_all_project_info() so both return each plate in the same shape.
    """
    import dataclasses
    import json

    if dataclasses.is_dataclass(info):
        result = json.loads(json.dumps(_to_dict(info), default=str))
    else:
        result = info if isinstance(info, dict) else {"info": str(info)}

    if not include_images:
        meta = result.get("metadata")
        if isinstance(meta, dict):
            if "topimg" in meta:
                meta["topimg"] = "[image omitted — use get_plate_topview to fetch]"
            if "thumbnail" in meta:
                meta["thumbnail"] = "[image omitted — use get_plate_thumbnail to fetch]"
    return result


def _find_file_in_tree(tree: dict, target_path: str) -> dict | None:
    """Recursively search the sdcard file tree for an entry matching target_path."""
    log.debug("_find_file_in_tree: searching for %s", target_path)
    if not tree:
        return None
    if tree.get("id") == target_path or tree.get("name") == target_path:
        return tree
    for child in tree.get("children", []):
        result = _find_file_in_tree(child, target_path)
        if result:
            return result
    return None


def list_sdcard_files(name: str, path: str = "/", cached: bool = False) -> dict:
    """
    Return the SD card directory listing for the named printer, whole or one subtree.

    WHEN to use: see what files and folders are on the printer's SD card, or narrow the
    listing to one folder such as ``/cache/`` or ``/model/`` to keep the response small.

    Sibling disambiguation: ``list_sdcard_files`` returns a directory tree, while
    ``get_file_info`` returns the single entry for one known path. ``refresh_sdcard`` only
    re-reads the card into the cache and returns no listing; it matters when you then call
    this tool with ``cached=True``. ``get_3mf_entry_by_name`` and ``get_3mf_entry_by_id``
    search the .3mf-only tree for one entry.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        path: ``"/"`` (default) returns the full top-level tree. A subdirectory returns only
            that subtree, which is much smaller. The subtree is the first depth-first node
            whose ``id`` or ``name`` equals ``path`` exactly. Directory ids carry a TRAILING
            SLASH, so use ``"/cache/"`` or the bare name ``"cache"``; ``"/cache"`` matches
            nothing and returns ``Path not found``.
        cached: False (default) performs a live FTPS fetch from the printer: guaranteed
            current, but it needs an active connection and takes a moment. True returns the
            in-memory cached copy immediately without contacting the printer, for when stale
            data is acceptable and low latency matters. The cache is populated by the most
            recent live listing (this tool with ``cached=False``, or ``refresh_sdcard``). If
            the cache has never been populated, or the last listing was reported as failed
            (the library then clears it), the call returns the
            ``Failed to retrieve SD card contents`` error. A live listing returns the same
            error when the listing failed (a timeout, a dropped connection, or any folder that
            could not be listed): a failed listing is never returned as an empty tree. An
            EMPTY ``children`` list therefore means the card, or that folder, really is
            empty; a folder the printer refuses to list also reads as empty.

    Returns:
        ``{"path": <path>, "contents": <node>}`` on success, where a node is
        ``{"id": <full SD card path>, "name", "size" (bytes), "timestamp", "children"
        (directories only)}``. A response whose JSON exceeds 300 characters (in practice
        almost every real listing) comes back instead as the gzip envelope
        ``{"compressed": True, "encoding": "gzip+base64", "original_size_bytes",
        "compressed_size_bytes", "data"}``. Errors are ``{"error": str}``:
        ``"Printer '<name>' not connected"``, ``"Failed to retrieve SD card contents"``,
        ``{"error": "Path not found: <path>", "path": <path>}``, or
        ``"Error listing SD card: <exception>"``.

    Notes:
        Use it as a hierarchy: ``list_sdcard_files(name)`` for the full top-level tree,
        ``list_sdcard_files(name, "/cache/")`` for only the /cache subtree,
        ``list_sdcard_files(name, "/model/")`` for only the /model subtree.
        A live listing (``cached=False``) also repopulates the printer library's cached tree.
        To decompress the gzip envelope:
        ``import gzip, json, base64; data = json.loads(gzip.decompress(base64.b64decode(r["data"])))``.
        If the compressed envelope itself exceeds the MCP response limit, use the HTTP
        fallback ``GET /api/get_sdcard_contents?printer=<name>``, or reduce scope by listing a
        specific subdirectory (for example ``path="/cache/"``).
    """
    log.debug("list_sdcard_files: called for name=%s path=%s cached=%s", name, path, cached)
    from tools._response import compress_if_large
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("list_sdcard_files: printer not connected: %s", name)
        return _no_printer(name)
    try:
        if cached:
            log.debug("list_sdcard_files: returning cached_sd_card_contents for %s", name)
            contents = printer.cached_sd_card_contents
        else:
            log.debug("list_sdcard_files: calling printer.get_sdcard_contents() for %s", name)
            contents = printer.get_sdcard_contents()
        if contents is None:
            log.debug("list_sdcard_files: → error: no contents for %s", name)
            return {"error": "Failed to retrieve SD card contents"}
        if path and path != "/":
            subtree = _find_file_in_tree(contents, path)
            if subtree is None:
                log.debug("list_sdcard_files: path not found: %s", path)
                return {"error": f"Path not found: {path}", "path": path}
            contents = subtree
        log.debug("list_sdcard_files: success for %s path=%s", name, path)
        return compress_if_large({"path": path, "contents": contents})
    except Exception as e:
        log.error("list_sdcard_files: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error listing SD card: {e}"}


def get_file_info(name: str, file_path: str) -> dict:
    """
    Return metadata for a specific file on the printer's SD card.

    WHEN to use: check whether one known file or folder exists on the SD card and read its
    size and timestamp, without pulling the whole tree into your context.

    Sibling disambiguation: ``get_file_info`` returns the one entry for a path you already
    know; ``list_sdcard_files`` returns a directory tree. ``get_3mf_entry_by_id`` does the
    same exact-path lookup but only over the .3mf-only tree.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the file or folder (for example
            ``"/cache/part.gcode.3mf"``). The first depth-first node whose ``id`` or ``name``
            equals this value exactly is returned, so a bare filename also matches. A folder's
            ``id`` ends with a trailing slash, so pass ``"/cache/"`` or the bare name
            ``"cache"``; ``"/cache"`` matches nothing.

    Returns:
        ``{"file": <entry>}`` on success, where the entry carries the file attributes
        ``id`` (full SD card path), ``name``, ``size`` and ``timestamp``; a directory entry
        also carries ``children``. Errors are ``{"error": str}``:
        ``"Printer '<name>' not connected"``, ``"Failed to retrieve SD card contents"``
        (the library reported the listing as failed, see Notes), ``"File not found:
        <file_path>"``, or ``"Error getting file info: <exception>"``.

    Notes:
        Every call retrieves the full SD card listing live over FTPS (which also repopulates
        the printer library's cached tree) and then searches it. A listing that failed (a
        timeout, a dropped connection, or a folder that could not be listed) gives the
        ``Failed to retrieve`` error, so ``File not found`` means the file is not on the card.
    """
    log.debug("get_file_info: called for name=%s file_path=%s", name, file_path)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_file_info: printer not connected: %s", name)
        return _no_printer(name)
    try:
        log.debug("get_file_info: calling printer.get_sdcard_contents() for %s", name)
        contents = printer.get_sdcard_contents()
        if contents is None:
            log.debug("get_file_info: → error: no contents for %s", name)
            return {"error": "Failed to retrieve SD card contents"}
        entry = _find_file_in_tree(contents, file_path)
        if entry is None:
            log.debug("get_file_info: → not found: %s", file_path)
            return {"error": f"File not found: {file_path}"}
        log.debug("get_file_info: → found %s", file_path)
        return {"file": entry}
    except Exception as e:
        log.error("get_file_info: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error getting file info: {e}"}


def get_3mf_entry_by_name(name: str, target_name: str) -> dict:
    """
    Search the SD card 3MF file tree for the entry with a given filename.

    WHEN to use: you know a .3mf filename but not its full SD card path, and need the path
    (the ``id`` field) or the file's size and timestamp.

    Sibling disambiguation: ``get_3mf_entry_by_name`` matches on the filename;
    ``get_3mf_entry_by_id`` matches on the full SD card path. ``list_sdcard_files`` returns
    the whole tree of every file, not just .3mf files.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        target_name: Filename only, not a full path, for example ``"my_project.gcode.3mf"``
            or ``"part.3mf"``. Matching is case-sensitive and exact: no wildcards or partial
            matches.

    Returns:
        ``{"entry": <node>}`` on success, where the node has ``id`` (full SD card path),
        ``name``, ``size`` (bytes), ``timestamp`` (epoch) and ``children`` (directories
        only). Errors are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Failed to retrieve SD card contents"``, ``"Not found: <target_name>"`` when no
        entry matches, or ``"Error searching SD card: <exception>"``.

    Notes:
        The search is a depth-first walk of the tree returned by ``get_sdcard_3mf_files()``,
        which runs a live FTPS listing and refreshes the printer library's cache before
        filtering. That tree holds only directories and files whose path ends in ``.3mf``.
        The first match wins.
    """
    log.debug("get_3mf_entry_by_name: called for name=%s target_name=%s", name, target_name)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_3mf_entry_by_name: printer not connected: %s", name)
        return _no_printer(name)
    try:
        from bpm.bambuproject import get_3mf_entry_by_name as _bpm_search
        tree = printer.get_sdcard_3mf_files()
        if tree is None:
            log.debug("get_3mf_entry_by_name: → error: no sd card contents for %s", name)
            return {"error": "Failed to retrieve SD card contents"}
        result = _bpm_search(tree, target_name)
        if result is None:
            log.debug("get_3mf_entry_by_name: → not found: %s", target_name)
            return {"error": f"Not found: {target_name}"}
        log.debug("get_3mf_entry_by_name: → found %s at %s", target_name, result.get("id"))
        return {"entry": result}
    except Exception as e:
        log.error("get_3mf_entry_by_name: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error searching SD card: {e}"}


def get_3mf_entry_by_id(name: str, target_id: str) -> dict:
    """
    Search the SD card 3MF file tree for the entry with a given full path.

    WHEN to use: you already have a full SD card path (for example from
    ``list_sdcard_files``) and need that entry's size and timestamp, or need to confirm it
    is a .3mf on the card.

    Sibling disambiguation: ``get_3mf_entry_by_id`` matches on the full SD card path;
    ``get_3mf_entry_by_name`` matches on the filename alone. ``get_file_info`` does an
    exact-path lookup over the full tree rather than the .3mf-only tree.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        target_id: Full SD card path as returned by ``list_sdcard_files``, for example
            ``"/cache/my_project.gcode.3mf"`` or ``"/model/part.3mf"``. Directory entries
            have a trailing slash: ``"/cache/"``. Matching is case-sensitive and exact.

    Returns:
        ``{"entry": <node>}`` on success, where the node has ``id`` (full SD card path),
        ``name``, ``size`` (bytes), ``timestamp`` (epoch) and ``children`` (directories
        only). Errors are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Failed to retrieve SD card contents"``, ``"Not found: <target_id>"`` when no entry
        matches, or ``"Error searching SD card: <exception>"``.

    Notes:
        The search is a depth-first walk of the tree returned by ``get_sdcard_3mf_files()``,
        which runs a live FTPS listing and refreshes the printer library's cache before
        filtering. That tree holds only directories and files whose path ends in ``.3mf``.
    """
    log.debug("get_3mf_entry_by_id: called for name=%s target_id=%s", name, target_id)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_3mf_entry_by_id: printer not connected: %s", name)
        return _no_printer(name)
    try:
        from bpm.bambuproject import get_3mf_entry_by_id as _bpm_search
        tree = printer.get_sdcard_3mf_files()
        if tree is None:
            log.debug("get_3mf_entry_by_id: → error: no sd card contents for %s", name)
            return {"error": "Failed to retrieve SD card contents"}
        result = _bpm_search(tree, target_id)
        if result is None:
            log.debug("get_3mf_entry_by_id: → not found: %s", target_id)
            return {"error": f"Not found: {target_id}"}
        log.debug("get_3mf_entry_by_id: → found %s", target_id)
        return {"entry": result}
    except Exception as e:
        log.error("get_3mf_entry_by_id: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error searching SD card: {e}"}


def get_project_info(name: str, file_path: str, plate_num: int = 1, include_images: bool = False) -> dict:
    """
    Return 3MF metadata and thumbnail info for one plate of a project file on the SD card.

    WHEN to use: read a .3mf plate's filaments, AMS mapping placeholder and object bounding
    boxes before choosing a plate, building a print summary, or calling ``print_file``.

    Sibling disambiguation: ``get_project_info`` returns one plate per call;
    ``get_all_project_info`` returns every plate in the file in one call. The image tools
    ``get_plate_thumbnail`` and ``get_plate_topview`` fetch a single plate's picture, and
    ``get_current_job_project_info`` resolves the file and plate of the active job for you.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        plate_num: Plate to parse (default 1). If the requested plate does not exist in the
            .3mf, the library SILENTLY falls back to the file's first available plate: compare
            the returned ``plate_num`` with the one you asked for, and read ``plates`` to see
            which plates exist.
        include_images: False (default) omits the ``metadata.topimg`` and
            ``metadata.thumbnail`` image fields to keep the response small. True includes
            both as raw base64 data URIs (large). Use True only when the AI agent needs to
            process the image bytes directly (vision analysis, comparison) or is describing
            the image on the human's behalf; a human cannot see raw base64 in a chat or
            terminal, so to let the human *view* the plates call ``open_plate_viewer``.

    Returns:
        The serialized project info for the plate: ``id``, ``name``, ``size``, ``timestamp``,
        ``md5``, ``plate_num``, ``plates`` and ``metadata`` (see Notes for the key fields).
        Errors are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Could not retrieve project info for '<file_path>'"``, or
        ``"Error getting project info: <exception>"``.

    Notes:
        The .3mf file is created by BambuStudio or OrcaSlicer, the slicing applications that
        turn .STL/.3MF model files into printable G-code and package everything into a .3mf
        project file. The tool parses the requested plate using a local metadata cache. A
        cache HIT still performs a live FTPS listing of the whole SD card to validate the cache
        (the printer must be reachable) and repopulates the printer library's cached trees;
        only the .3mf download is skipped. On a miss the .3mf is downloaded over FTPS and the
        cache is written. The same applies to every tool that reads project info.
        ``get_all_project_info`` and ``open_plate_viewer`` pay for that listing once per call,
        because the plates after the first are served from the cached listing.

        Three ``metadata`` keys say which external spool holder each filament prints from
        (bpm 1.0.4 and later; a daemon running an older bpm omits them). All are lists indexed
        by ``filament id - 1``. ``filament_extruders`` is the slicer's 1-based logical extruder
        per filament, from ``slice_info.config``; it is empty when the slicer wrote none.
        ``physical_extruder_map`` is the physical extruder (0 main, 1 deputy) per logical
        extruder, from the plate gcode config block; it is empty when absent, and is ``[1, 0]``
        on an H2D, so logical extruder 1 is the LEFT one. ``external_spool_trays`` is the wire
        id of the external holder feeding each filament: 255 main (right), 254 deputy (left),
        -1 unused; a single-nozzle printer reports 255 for every used filament although its
        telemetry calls its one holder tray 254, so do not match this list to a spool's
        ``slot_id``. It is empty when a dual-nozzle plate has no extruder map, and it is
        derived on every read, never cached. ``print_file`` with ``use_ams=False`` uses the
        same derivation, so a caller passes no holder.

        Multi-level call hierarchy:
          Level 1: ``get_project_info(name, file, 1)`` returns ``{plates: [...], ...}`` (index):
          the file's real plate numbers, which may be sparse, such as [1,5,6,12].
          Level 2: ``get_project_info(name, file, N)`` for one of those numbers returns
          per-plate metadata and bbox_objects.
          Level 3: ``get_plate_thumbnail(name, file, N)`` returns just the isometric image;
          ``get_plate_topview(name, file, N)`` returns just the top-down image.

        Key fields in the returned dict:
        - ``plates``: list of all plate numbers in the file. They are not necessarily
          contiguous or 1-based (e.g. [1,5,6,12] or [10]). Iterate over that list, never
          over ``range(1, len(plates) + 1)``, and call ``get_project_info`` once per listed
          plate to retrieve all plates (or use ``get_all_project_info``).
        - ``metadata.filament``: list of ``{"id": int (1-based), "type": str, "color": str}``
          for the plate's filaments, the colour being a hex string such as "#RRGGBB". These ids index the ``ams_mapping`` array
          that ``print_file`` and ``preview_ams_mapping`` use.
        - ``metadata.ams_mapping``: a filament-id PLACEHOLDER only, never a real slot
          assignment; do not pass it to ``print_file``.
        - ``metadata.map.bbox_objects``: list of ``{name, ...}`` dicts for objects on this
          plate. Filter out entries whose name contains ``wipe_tower`` to get the
          human-readable part list.
        - ``metadata.topimg``: present only when ``include_images=True``. Complete base64
          data URI (``data:image/png;base64,...``). Use DIRECTLY as an img src.
        - ``metadata.thumbnail``: present only when ``include_images=True``. Isometric
          thumbnail data URI. Use DIRECTLY as an img src.
        - With ``include_images=False`` those two fields are replaced by an omission marker
          string naming the tool to fetch them.

        Coordinate system for bbox fields:
        - bbox values are [x_min, y_min, x_max, y_max] in millimetres, absolute bed position.
        - Origin (0,0) is the BOTTOM-LEFT of the build plate (slicer convention).
        - To map to image pixel coords (origin top-left): flip Y, pixel_y = img_height -
          (y_mm / bed_h * img_height).
        - Apply uniform scale: scale = min(img_w / bed_w, img_h / bed_h); add centring offsets.
        - Bed dimensions by model (mm, W x H): H2D/H2S=350x320,
          X1C/X1/X1E/P1S/P1P/P2S/A1=256x256, A1_MINI=180x180.
        - Use ``printer.config.printer_model.value`` to get the model string for dimension lookup.

        Cross-tool link: ``bbox_objects[].id`` values are the identify_id integers required by
        ``skip_objects``. Filter bbox_objects to exclude entries whose name contains
        ``wipe_tower`` to get human-readable part names.

        When ``include_images=True`` the response carries raw base64 data URIs which may
        exceed the CLI inline display limit. If output is truncated, use the HTTP fallback:
        ``GET http://localhost:{api_port}/api/get_3mf_props_for_file?printer={name}&file={file_path}&plate={plate_num}``.
        Call ``kb_get('bambu-http-files')`` for full route docs. Pre-authorized, no human
        permission needed.
    """
    log.debug("get_project_info: called for name=%s file_path=%s plate_num=%s include_images=%s", name, file_path, plate_num, include_images)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_project_info: printer not connected: %s", name)
        return _no_printer(name)
    try:
        from bpm.bambuproject import get_project_info as _get_project_info
        log.debug("get_project_info: calling _get_project_info for %s", name)
        info = _get_project_info(file_path, printer, plate_num=plate_num)
        if info is None:
            log.debug("get_project_info: → error: no info for %s plate=%s", file_path, plate_num)
            return {"error": f"Could not retrieve project info for '{file_path}'"}
        log.debug("get_project_info: info retrieved for %s", name)
        result = _serialize_project_info(info, include_images)

        log.debug("get_project_info: → result for %s plate=%s include_images=%s", file_path, plate_num, include_images)
        return result
    except Exception as e:
        log.error("get_project_info: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error getting project info: {e}"}


def get_all_project_info(name: str, file_path: str, include_images: bool = False) -> dict | list:
    """
    Return 3MF metadata for every plate in a project file, in a single call.

    WHEN to use: you need all plates of a multi-plate project at once, for example to survey
    the file or build a plate picker, instead of one round trip per plate.

    Sibling disambiguation: ``get_all_project_info`` is the batch counterpart of
    ``get_project_info``, which returns one plate per call. Use ``get_plate_thumbnail`` or
    ``get_plate_topview`` to fetch a single plate's image on demand.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        include_images: Behaves exactly as in ``get_project_info``. False (default) omits the
            ``metadata.topimg`` and ``metadata.thumbnail`` fields per plate to keep the
            response small; True includes both data URIs for every plate (large).

    Returns:
        A list of per-plate dicts on success, each shaped exactly like a single
        ``get_project_info`` response, ordered by plate number. Errors are a dict, not a
        list: ``{"error": "Printer '<name>' not connected"}``,
        ``{"error": "Could not retrieve project info for '<file_path>'"}``, or
        ``{"error": "Error getting all project info: <exception>"}``.

    Notes:
        The tool fetches the .3mf's actual plate set. Plate numbers are not assumed to be
        contiguous: a .3mf may contain a sparse set of plates (e.g. [1,5,6,7,8,12,15]) if
        plates were deleted in the slicer, and only plates that genuinely exist are returned.
        The plate set is bounded by the library's ``max_plates`` safety ceiling of 30: a plate
        numbered above 30 is ignored and does not appear in the result. The .3mf may be
        downloaded over FTPS once and its metadata cache written. Each plate's ``metadata``
        carries ``filament_extruders``, ``physical_extruder_map`` and ``external_spool_trays``
        exactly as ``get_project_info`` describes them.
    """
    log.debug("get_all_project_info: called for name=%s file_path=%s include_images=%s", name, file_path, include_images)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_all_project_info: printer not connected: %s", name)
        return _no_printer(name)
    try:
        from bpm.bambuproject import get_all_project_info as _get_all_project_info
        log.debug("get_all_project_info: calling _get_all_project_info for %s", name)
        infos = _get_all_project_info(file_path, printer)
        if not infos:
            log.debug("get_all_project_info: → error: no plates for %s", file_path)
            return {"error": f"Could not retrieve project info for '{file_path}'"}
        plates = [_serialize_project_info(info, include_images) for info in infos]
        log.debug("get_all_project_info: → %d plate(s) for %s", len(plates), name)
        return plates
    except Exception as e:
        log.error("get_all_project_info: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error getting all project info: {e}"}


def get_plate_thumbnail(
    name: str,
    file_path: str,
    plate_num: int = 1,
    quality: str = "standard",
) -> dict:
    """
    Return the isometric thumbnail image for a single plate in a 3MF project file.

    WHEN to use: the AI agent itself is the consumer of the image, either to describe or
    analyze the plate on the human's behalf ("what does it look like?", "describe the plate",
    "is there anything on it?") or to process the raw bytes directly (vision model input,
    pixel comparison, local image library). It is the separated visual sub-call of
    ``get_project_info`` and returns only the thumbnail, without metadata or bbox objects.

    Sibling disambiguation: ``get_plate_thumbnail`` returns the isometric view and
    ``get_plate_topview`` returns the top-down view of the same plate. When the human is the
    intended viewer ("show me", "open it", "let me see it") call ``open_plate_viewer`` for all
    plates or ``open_plate_layout`` for an annotated single-plate view; returning a raw
    ``data_uri`` to a human in a chat or terminal is never the right choice. For
    ``print_file`` pre-flight and print job prep, always use ``open_plate_viewer``, never
    this tool (see ``print_file``).

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        plate_num: Plate number (default 1). An absent plate silently yields the file's first
            available plate; check ``plates`` from ``get_project_info`` first.
        quality: Image size and JPEG compression tier, default ``"standard"``. The dimensions
            are a MAXIMUM bounding box with aspect ratio preserved and no upscaling, so the
            returned ``width``/``height`` are usually smaller: ``"preview"`` = max 320x180 at
            JPEG q=65, ``"standard"`` = max 640x360 at q=75, ``"full"`` = original dimensions
            at q=85. An unknown tier falls back to ``"standard"``. Read ``width`` and
            ``height`` from the result.

    Returns:
        ``{"data_uri": <complete data:image/jpeg;base64,... string, embed directly as an img
        src>, "plate_num": <the plate_num you PASSED, not necessarily the plate rendered>,
        "quality": <tier as passed>, "width": <int>, "height": <int>}`` on success. Errors
        are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Could not retrieve project info for '<file_path>'"``,
        ``"No thumbnail image available for plate <plate_num>"``, or
        ``"Error retrieving plate image: <exception>"``.

    Notes:
        The result is a raw base64 data URI, which may exceed the CLI inline display limit.
        If output is truncated, call ``kb_get('bambu-http-files')`` for the equivalent HTTP
        endpoints, then use bash/curl to retrieve the data directly; this is pre-authorized
        and requires no human permission. The .3mf may be downloaded over FTPS and its
        metadata cache written as a side effect.
    """
    log.debug("get_plate_thumbnail: called for name=%s file_path=%s plate_num=%s quality=%s", name, file_path, plate_num, quality)
    return _get_plate_image(name, file_path, plate_num, quality, image_key="thumbnail")


def get_plate_topview(
    name: str,
    file_path: str,
    plate_num: int = 1,
    quality: str = "standard",
) -> dict:
    """
    Return the top-down view image for a single plate in a 3MF project file.

    WHEN to use: the AI agent itself is the consumer of the image, either to describe or
    analyze the plate on the human's behalf ("what does it look like?", "describe the
    plate", "is there anything on it?") or to process the raw bytes directly (vision model
    input, pixel comparison, local image library). It is the separated visual sub-call of
    ``get_project_info`` and returns only the top-down view, without metadata or bbox
    objects.

    Sibling disambiguation: ``get_plate_topview`` returns the top-down view and
    ``get_plate_thumbnail`` returns the isometric view of the same plate. When the human is
    the intended viewer ("show me", "open it", "let me see it") call ``open_plate_viewer``
    for all plates or ``open_plate_layout`` for an annotated single-plate view; returning a
    raw ``data_uri`` to a human in a chat or terminal is never the right choice. For
    ``print_file`` pre-flight and print job prep, always use ``open_plate_viewer``, never
    this tool (see ``print_file``).

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        plate_num: Plate number (default 1). An absent plate silently yields the file's first
            available plate; check ``plates`` from ``get_project_info`` first.
        quality: Image size and JPEG compression tier, default ``"standard"``. The dimensions
            are a MAXIMUM bounding box with aspect ratio preserved and no upscaling, so the
            returned ``width``/``height`` are usually smaller: ``"preview"`` = max 320x180 at
            JPEG q=65, ``"standard"`` = max 640x360 at q=75, ``"full"`` = original dimensions
            at q=85. An unknown tier falls back to ``"standard"``. Read ``width`` and
            ``height`` from the result.

    Returns:
        ``{"data_uri": <complete data:image/jpeg;base64,... string, embed directly as an img
        src>, "plate_num": <the plate_num you PASSED, not necessarily the plate rendered>,
        "quality": <tier as passed>, "width": <int>, "height": <int>}`` on success. Errors
        are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Could not retrieve project info for '<file_path>'"``,
        ``"No topimg image available for plate <plate_num>"``, or
        ``"Error retrieving plate image: <exception>"``.

    Notes:
        The result is a raw base64 data URI, which may exceed the CLI inline display limit.
        If output is truncated, call ``kb_get('bambu-http-files')`` for the equivalent HTTP
        endpoints, then use bash/curl to retrieve the data directly; this is pre-authorized
        and requires no human permission. The .3mf may be downloaded over FTPS and its
        metadata cache written as a side effect.
    """
    log.debug("get_plate_topview: called for name=%s file_path=%s plate_num=%s quality=%s", name, file_path, plate_num, quality)
    return _get_plate_image(name, file_path, plate_num, quality, image_key="topimg")


def _get_plate_image(
    name: str,
    file_path: str,
    plate_num: int,
    quality: str,
    image_key: str,
) -> dict:
    """Shared implementation for get_plate_thumbnail and get_plate_topview."""
    printer = session_manager.get_printer(name)
    if printer is None:
        return _no_printer(name)
    try:
        import base64
        import io
        from bpm.bambuproject import get_project_info as _get_project_info
        import dataclasses
        import json
        from enum import Enum
        from tools._response import resize_image_to_tier

        def _to_dict(o):
            if isinstance(o, Enum):
                return o.name
            if dataclasses.is_dataclass(o) and not isinstance(o, type):
                return {f.name: _to_dict(getattr(o, f.name)) for f in dataclasses.fields(o)}
            if isinstance(o, dict):
                return {k: _to_dict(v) for k, v in o.items()}
            if isinstance(o, (list, tuple)):
                return [_to_dict(v) for v in o]
            return o

        info = _get_project_info(file_path, printer, plate_num=plate_num)
        if info is None:
            return {"error": f"Could not retrieve project info for '{file_path}'"}
        if dataclasses.is_dataclass(info):
            result = json.loads(json.dumps(_to_dict(info), default=str))
        else:
            result = info if isinstance(info, dict) else {}

        meta = result.get("metadata", {})
        data_uri = meta.get(image_key, "")
        if not data_uri:
            return {"error": f"No {image_key} image available for plate {plate_num}"}

        # data_uri is "data:image/png;base64,<b64>"
        raw_b64 = data_uri.split(",", 1)[1]
        img_bytes = base64.b64decode(raw_b64)
        jpeg_bytes, w, h = resize_image_to_tier(img_bytes, quality)
        jpeg_uri = "data:image/jpeg;base64," + base64.b64encode(jpeg_bytes).decode("ascii")

        log.debug("_get_plate_image: %s plate=%s quality=%s → %dx%d %d bytes", image_key, plate_num, quality, w, h, len(jpeg_bytes))
        return {"data_uri": jpeg_uri, "plate_num": plate_num, "quality": quality, "width": w, "height": h}
    except SdCardBusy as e:
        # The job monitor asks every minute during a print; the refusal is expected there.
        log.debug("_get_plate_image: %s: %s", name, e)
        return {"error": f"Error retrieving plate image: {e}"}
    except Exception as e:
        log.error("_get_plate_image: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error retrieving plate image: {e}"}


def prepare_print_job(part: str) -> dict:
    """
    Prepare one part for printing: run the server's configured preparer and admit the job it
    produces only if that run passes.

    WHEN to use: STEP 1 of every print. Only a job returned here can be uploaded as a printable
    file (``upload_file``) and then started (``print_file``). No caller-supplied report, path
    or flag can stand in for it.

    The server runs the fixed command an operator configured in its own environment
    (``BAMBU_PREFLIGHT_COMMAND_JSON``) with a new output directory it chose and ``part``; for
    the Soyrack guide that preparer renders the CAD, slices it with the installed Bambu Studio
    and runs its geometry and support checks. The job is admitted only when the process exits 0
    and its ``preflight.json`` says ``schema_version`` 2, ``geometry_policy_version`` 1,
    ``pass`` true, ``state`` complete, exactly this part with no problems, ``geometry.pass``
    true from policy 1, and a ``target`` (printer_model, one nozzle_diameter, plate_type)
    qualified for that policy (a single-nozzle Bambu Lab P1S today) which the job's own plate
    G-code also declares; and the job's SHA-256 matches what the server computes itself. It
    sends nothing to any printer.

    Sibling disambiguation: ``prepare_print_job`` creates the admitted job on this host;
    ``upload_file`` copies it to the SD card; ``print_file`` starts it.

    Args:
        part: One part name known to the preparer, matching ``^[a-z0-9][a-z0-9_-]{0,63}$``
            (for example ``drive_tray``).

    Returns:
        Success: ``{"success": True, "receipt_id", "part", "job_path", "sha256", "md5", "size",
        "plates", "plate_gcode_bytes", "target": {"printer_model", "nozzle_diameter", "plate_type", "bed_type"},
        "run_dir", "report_sha256", "prepared_at", "report": <summary copied from the
        preparer's report, incl. command, command_sha256, slicer, minutes, grams, watch>,
        "valid_until": "server restart", "next": <the upload call to make>}``. Pass
        ``job_path`` unchanged as ``upload_file``'s ``local_path``.
        Refusals are ``{"error": code, "detail": str, ...}`` with code ``invalid_part``,
        ``preflight_unconfigured``, ``preflight_config_invalid``, ``preflight_command_untrusted``,
        ``preparation_in_progress``, ``preflight_failed`` / ``preflight_timeout`` (with ``log``,
        ``log_tail`` and the preparer's ``report`` problems when present) or
        ``preflight_rejected`` (the report or job broke an admission rule).

    Notes:
        Receipts live in server memory only: a server restart forgets them, so prepare again.
        A job file changed after preparation is refused at upload. Runs are kept under
        ``~/.bambu-mcp/preflight/`` (newest 100). One preparation runs at a time.
    """
    log.debug("prepare_print_job: called for part=%s", part)
    import print_admission
    try:
        return print_admission.prepare(part)
    except print_admission.Refused as r:
        log.warning("prepare_print_job: %s refused: %s", part, r)
        return r.as_dict()


def _guarded_upload(name: str, printer, local_path: str, remote_path: str) -> dict:
    """Upload under the admission rules (shared with POST /api/upload_file_to_printer).

    Raises print_admission.Refused when refused; returns the success payload otherwise.
    """
    import shutil
    import print_admission as pa
    import storage_space
    storage_space.protect_record(pa.normalize_remote(remote_path))
    with pa.file_lock(name):
        if not (pa.is_job_name(local_path) or pa.is_job_name(remote_path)):
            # Not a printable job either way. It cannot become one later: rename refuses a
            # job-like destination for a non-job source, and print_file needs an admission.
            remote = pa.normalize_remote(remote_path)
            return {"success": True, "remote_path": remote, "contents": printer.upload_sdcard_file(local_path, remote)}
        receipt, remote, snapshot, tmpdir = pa.snapshot_for_upload(local_path, remote_path)
        try:
            # Before anything changes: the card must hold the job and what the printer unpacks from it.
            storage = storage_space.require_for_upload(name, printer, receipt)
            pa.drop(name, remote, "replaced by a new upload")
            warning = None
            try:
                contents = printer.upload_sdcard_file(snapshot, remote)
            except Exception as exc:
                # The transfer may have finished before the metadata parse or listing failed.
                contents, warning = None, f"{type(exc).__name__}: {exc}"
                try:
                    sha, _, _, _, verify_dir = pa.remote_digest(printer, remote)
                    shutil.rmtree(verify_dir, ignore_errors=True)
                except Exception as verify_exc:
                    sha, warning = None, f"{warning}; verification download failed: {verify_exc}"
                if sha != receipt.sha256:
                    raise pa.Refused("upload_failed",
                                     f"The upload of {remote} did not complete ({warning}). The card may hold a "
                                     "partial or old file, which is NOT admitted. Run upload_file again.")
            admitted = pa.bind(name, remote, receipt, storage["card_id"])
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
    out = {"success": True, "remote_path": remote, "contents": contents, "admission": admitted.public(),
           "storage": storage}
    if warning:
        out["warning"] = (f"After the transfer bambu reported {warning}; the uploaded bytes were downloaded "
                          "again and match the prepared job, so it is admitted.")
    return out


def upload_file(
    name: str,
    local_path: str,
    remote_path: str,
    user_permission: bool = False,
) -> dict:
    """
    Upload a local file to the printer's SD card; a printable job only if it was prepared by
    ``prepare_print_job``.

    WHEN to use: STEP 2 of a print: copy the ``job_path`` returned by ``prepare_print_job`` to
    the card, then call ``print_file`` on the same remote path. Also copies non-printable
    files (anything not ending ``.3mf`` or ``.gcode``) for diagnostics.

    WRITE GUARD: writes the file to the printer's SD card over FTPS, replacing any existing
    file at that remote path. With ``user_permission`` False the tool changes nothing and
    returns the ``{"error": ...}`` refusal naming that consequence.

    ADMISSION: a printable job is any upload whose local or remote name ends ``.3mf`` or
    ``.gcode`` (case-insensitive). It is accepted only when ``local_path`` is the ``job_path``
    of a prepared job in this server's lifetime and still has the prepared SHA-256: the server
    copies it to a private snapshot while hashing and uploads exactly those bytes to a remote
    path ending ``.3mf``. Then that remote path is bound to the job for ONE ``print_file``
    start. Any earlier binding for the same path is dropped before the transfer, and a failed
    transfer binds nothing. Arbitrary sliced files, edited jobs and jobs from a previous server
    run are refused; there is no override.

    STORAGE: before a prepared job is transferred, the server lists the whole card and reads
    its ``/bambu-mcp-card.json`` record (written by ``ops/attest_sd_card.py`` on a computer:
    the printer reports neither capacity nor free space). Available = the recorded data area
    minus every listed file and folder in whole clusters. It must cover the job, the largest
    plate G-code the printer will unpack and a 64 MiB reserve; a file being replaced is not
    credited. The printer must have reported a normal SD card on this connection. Otherwise
    nothing is written. This server never deletes files to make room.

    Sibling disambiguation: ``upload_file`` copies a file from this host to the printer;
    ``download_file`` copies the other way, from the printer to this host.
    ``rename_sdcard_file`` moves a file that is already on the card without re-uploading it.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        local_path: Full path of the file on this host to upload. For a printable job, the
            ``job_path`` from ``prepare_print_job``. Other files are not constrained to any
            directory.
        remote_path: Full destination path on the printer's SD card (a leading ``/`` is
            added; ``.`` and ``..`` segments are refused). A prepared job needs a ``.3mf``
            name, for example ``/drive_tray.gcode.3mf``.
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, "remote_path": <normalized remote path>, "contents": <refreshed SD
        card tree>}`` on success, plus ``"admission": {"receipt_id", "part", "remote_path",
        "sha256", "plates", "target", "card_id"}`` and ``"storage": {"card_id", "attested_at",
        "data_bytes", "cluster_bytes", "listed_allocated_bytes", "files", "folders",
        "available_bytes", "listing_s", "required_bytes",
        "required": {"job", "unpacked_gcode", "reserve"}, "sd_card": {"state", "source",
        "age_s"}}`` for a prepared job. ``contents`` is the printer library's SD card
        listing taken after the upload; null when that listing failed. A prepared job whose
        post-transfer step failed is downloaded again and admitted only if its bytes match,
        with a ``"warning"``. Errors are ``{"error": str}`` (permission refusal, ``"Printer
        '<name>' not connected"``, ``"Error uploading file: <exception>"``) or ``{"error":
        code, "detail": str}`` with code ``storage_record_protected`` (the SD card record
        ``/bambu-mcp-card.json`` is written only by ``ops/attest_sd_card.py``),
        ``job_not_prepared``, ``job_modified``,
        ``remote_not_project``, ``invalid_remote_path``, ``upload_failed``,
        ``file_operation_busy`` or, with nothing written, ``storage_state_unknown``,
        ``storage_no_card``, ``storage_card_abnormal``, ``storage_card_read_only``,
        ``storage_unattested`` (no card record), ``storage_unknown`` (record
        unreadable, or a folder or listing row the card listing cannot account for) or ``storage_insufficient`` (with ``storage``: available, required and
        their basis).

    Notes:
        A ``.3mf`` upload is parsed and cached by the printer library after the transfer, and
        the SD card is then re-listed. For a non-job file a failure there surfaces as ``"Error
        uploading file: ..."`` even though the file is ALREADY on the card; check with
        ``get_file_info`` before retrying.
    """
    log.debug("upload_file: called for name=%s local_path=%s remote_path=%s user_permission=%s", name, local_path, remote_path, user_permission)
    if not user_permission:
        log.debug("upload_file: permission denied for %s", name)
        return {"error": _permission_denied(
            "This would upload the local file to the printer's SD card, replacing any file already at that remote path."
        )}
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("upload_file: printer not connected: %s", name)
        return _no_printer(name)
    import print_admission
    try:
        result = _guarded_upload(name, printer, local_path, remote_path)
        log.debug("upload_file: → remote_path=%s admission=%s", result["remote_path"], result.get("admission"))
        return result
    except print_admission.Refused as r:
        log.warning("upload_file: refused for %s: %s", name, r)
        return r.as_dict()
    except Exception as e:
        log.error("upload_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error uploading file: {e}"}


def register_sd_card(name: str, registration_id: str, user_permission: bool = False) -> dict:
    """
    Record the size of the SD card in the printer on the card, from an operator registration.

    WHEN to use: only as the last step of ``ops/register_sd_card.py``, which an operator runs on
    this host with saved ``fsck.fat -n`` output of this card (checked on a computer) and a
    custody statement, and which calls this tool itself with the id it prints. This tool takes
    no size: the size comes from the fsck output in the registration, parsed again here. A card
    in a computer is recorded with ``ops/attest_sd_card.py`` instead (docs/storage-space.md).

    WRITE GUARD: writes ``/bambu-mcp-card.json`` (about 1 KB) to the printer's SD card
    over this server's own FTPS session, only while the printer reports no job, only when the
    card has no record yet. With ``user_permission`` False nothing is read or written.

    Sibling disambiguation: ``upload_file`` and ``rename_sdcard_file`` refuse the record's name
    (``storage_record_protected``); this is the only tool that writes it.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        registration_id: The 32-hex id ``ops/register_sd_card.py`` printed. Each registration is
            used once, by the first call naming it, and expires 15 minutes after it was made.
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"registered": True, "registration_id", "device", "fsck_used_clusters", "sd_card",
        "storage": {"card_id", "attested_at", "basis", "data_bytes", "cluster_bytes",
        "listed_allocated_bytes", "files", "folders", "available_bytes", "listing_s"}}``: the card
        as uploads and starts will now read it. Errors are ``{"error": code, "detail": str}`` with
        nothing written for ``registration_invalid``, ``registration_missing`` (none pending with
        that id: never made, already used), ``registration_store_untrusted``,
        ``storage_already_attested``, ``registration_inconsistent`` (the card lists more than the
        registered data area), ``storage_state_unknown``, ``storage_no_card``,
        ``storage_card_abnormal``, ``storage_card_read_only``, ``storage_unknown`` (card unreadable,
        listing incomplete, or the printer reports a job) and ``file_operation_busy``;
        ``registration_failed`` when the write, its read-back or the measurement after it failed (detail says what the card
        may hold); the ``_permission_denied`` refusal; ``"Printer '<name>' not connected"``.
    """
    log.debug("register_sd_card: called for name=%s registration_id=%s user_permission=%s", name, registration_id, user_permission)
    if not user_permission:
        return {"error": _permission_denied("This would write the SD card record /bambu-mcp-card.json to the card.")}
    printer = session_manager.get_printer(name)
    if printer is None:
        return _no_printer(name)
    import print_admission
    import storage_space
    try:
        return storage_space.register(name, printer, registration_id)
    except print_admission.Refused as r:
        log.warning("register_sd_card: %s refused: %s", name, r)
        return r.as_dict()


def download_file(
    name: str,
    remote_path: str,
    local_path: str,
    user_permission: bool = False,
) -> dict:
    """
    Download a file from the printer's SD card to the local filesystem.

    WHEN to use: copy a file off the printer's SD card onto this host, for example to inspect
    or back up a .3mf.

    WRITE GUARD: writes the downloaded file to ``local_path`` on this host, creating it or
    truncating and overwriting whatever file is already there. The file is created or
    truncated BEFORE the transfer begins, so a failed download (missing remote file, dropped
    connection) leaves ``local_path`` emptied or partly written even though the tool returns
    an error. ``local_path`` is not constrained to any directory on this tool. With
    ``user_permission`` False the tool changes nothing and returns the ``{"error": ...}``
    refusal naming that consequence.

    Sibling disambiguation: ``download_file`` copies from the printer to this host;
    ``upload_file`` copies the other way, from this host to the printer's SD card.
    ``list_sdcard_files`` and ``get_file_info`` only read the card's listing and do not
    transfer any file.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        remote_path: Full path of the file on the printer's SD card.
        local_path: Destination path on this host. Paths inside the server's prepared-job
            store (``~/.bambu-mcp/preflight/``) and SD card registration store
            (``~/.bambu-mcp/sd-card/``) are refused, so a download can never replace a prepared
            job, its report or an operator's card registration.
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, "remote_path": <remote_path>, "local_path": <local_path>}`` on
        success. Errors are ``{"error": str}``: the ``_permission_denied`` refusal when
        ``user_permission`` is False, ``"Printer '<name>' not connected"``, or
        ``"Error downloading file: <exception>"``; or ``{"error": "local_path_reserved",
        "detail": str}`` for a destination inside either store.
    """
    log.debug("download_file: called for name=%s remote_path=%s local_path=%s user_permission=%s", name, remote_path, local_path, user_permission)
    if not user_permission:
        log.debug("download_file: permission denied for %s", name)
        return {"error": _permission_denied(
            "This would write the SD card file to the local path on this host, overwriting any file already there."
        )}
    import print_admission
    import storage_space
    if print_admission.inside_store(local_path) or print_admission.inside_store(local_path, storage_space.registry_root()):
        return {"error": "local_path_reserved",
                "detail": f"{local_path} is inside the server's prepared-job or SD card registration store; choose "
                          "another destination. Nothing was downloaded."}
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("download_file: printer not connected: %s", name)
        return _no_printer(name)
    try:
        log.debug("download_file: calling printer.download_sdcard_file for %s", name)
        printer.download_sdcard_file(remote_path, local_path)
        log.debug("download_file: success for %s remote_path=%s", name, remote_path)
        log.debug("download_file: → remote_path=%s local_path=%s", remote_path, local_path)
        return {"success": True, "remote_path": remote_path, "local_path": local_path}
    except Exception as e:
        log.error("download_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error downloading file: {e}"}


def delete_file(
    name: str,
    remote_path: str,
    user_permission: bool = False,
) -> dict:
    """
    Delete a file or folder from the printer's SD card.

    WHEN to use: remove a file, or a whole folder, from the printer's SD card, for example to
    free space or clear out old jobs.

    WRITE GUARD: permanently deletes the file from the printer's SD card; a path ending in
    ``/`` deletes the folder and everything inside it, recursively. With ``user_permission``
    False the tool changes nothing and returns the ``{"error": ...}`` refusal naming that
    consequence.

    Sibling disambiguation: ``delete_file`` removes the data from the card;
    ``rename_sdcard_file`` only moves or renames a file, keeping its contents. ``create_folder``
    is the opposite operation for directories.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        remote_path: Full path on the SD card. A path ending in ``/`` is treated as a folder
            (``delete_sdcard_folder``, recursive); any other path is treated as a file
            (``delete_sdcard_file``).
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, "remote_path": <normalized remote path>, "contents": <SD card
        tree>}`` on success; ``contents`` is the printer library's cached SD card tree with the
        deleted entry removed (null if the cache is empty: never populated, or cleared by a
        listing the library reported as failed). Errors are ``{"error": str}``:
        the ``_permission_denied`` refusal when ``user_permission`` is False,
        ``"Printer '<name>' not connected"``, or ``"Error deleting file: <exception>"``; or
        ``{"error": "invalid_remote_path" | "file_operation_busy", "detail": str}``.

    Notes:
        A print admission (see ``upload_file``) for the deleted file, or for anything under a
        deleted folder, is dropped before the FTPS delete, even if the delete then fails.
    """
    log.debug("delete_file: called for name=%s remote_path=%s user_permission=%s", name, remote_path, user_permission)
    if not user_permission:
        log.debug("delete_file: permission denied for %s", name)
        return {"error": _permission_denied(
            "This would permanently delete the file from the printer's SD card (a path ending in '/' deletes the folder and everything in it)."
        )}
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("delete_file: printer not connected: %s", name)
        return _no_printer(name)
    import print_admission
    try:
        path, result = _guarded_delete(name, printer, remote_path)
        log.debug("delete_file: → remote_path=%s", path)
        return {"success": True, "remote_path": path, "contents": result}
    except print_admission.Refused as r:
        return r.as_dict()
    except Exception as e:
        log.error("delete_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error deleting file: {e}"}


def _guarded_delete(name: str, printer, remote_path: str):
    """Delete a file or folder, dropping admissions it invalidates first. Returns (path, tree).

    Shared with DELETE /api/delete_sdcard_file. Raises print_admission.Refused.
    """
    import print_admission as pa
    path = pa.normalize_remote(remote_path)
    with pa.file_lock(name):
        pa.drop(name, path, "deleted")
        if path.endswith("/"):
            return path, printer.delete_sdcard_folder(path)
        return path, printer.delete_sdcard_file(path)


def create_folder(
    name: str,
    path: str,
    user_permission: bool = False,
) -> dict:
    """
    Create a directory on the printer's SD card.

    WHEN to use: make a new folder on the printer's SD card, for example to organise uploads.

    WRITE GUARD: creates a new directory on the printer's SD card over FTPS (an FTPS mkdir).
    With ``user_permission`` False the tool changes nothing and returns the ``{"error": ...}``
    refusal naming that consequence.

    Sibling disambiguation: ``create_folder`` makes an empty directory; ``upload_file`` puts a
    file on the card, and ``delete_file`` (with a trailing ``/``) removes a directory.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        path: Full path of the directory to create on the SD card.
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, "path": <path>, "contents": <refreshed SD card tree>}`` on
        success; ``contents`` is the printer library's SD card listing taken after the
        creation. It is null when that listing failed; the folder itself was created either
        way, so the tool still returns success. Errors are ``{"error": str}``:
        the ``_permission_denied`` refusal when ``user_permission`` is False,
        ``"Printer '<name>' not connected"``, or ``"Error creating folder: <exception>"``.
    """
    log.debug("create_folder: called for name=%s path=%s user_permission=%s", name, path, user_permission)
    if not user_permission:
        log.debug("create_folder: permission denied for %s", name)
        return {"error": _permission_denied(
            "This would create a new directory on the printer's SD card."
        )}
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("create_folder: printer not connected: %s", name)
        return _no_printer(name)
    try:
        log.debug("create_folder: calling printer.make_sdcard_directory for %s", name)
        result = printer.make_sdcard_directory(path)
        log.debug("create_folder: success for %s path=%s", name, path)
        log.debug("create_folder: → path=%s", path)
        return {"success": True, "path": path, "contents": result}
    except Exception as e:
        log.error("create_folder: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error creating folder: {e}"}


# ── AMS mapping from live spools ────────────────────────────────────────────
# bpm's get_project_info() never yields real tray ids: the 3mf's
# `filament_maps` is the slicer's per-filament EXTRUDER assignment, and bpm
# replaces it with a filament-id placeholder ("1", "2", ...). The only source
# of a real mapping is what the printer last reported loaded. This block mirrors
# bambu-printer-app's Print3mfFileDialog (reconstructAmsMapping /
# getProtocolTrayId / toWireMapping / getMatchQuality) scoring. Two deliberate
# differences, because the dialog has a human at a dropdown and this tool does
# not: colour names are parsed with the full CSS3 table (bpm emits CSS3 names;
# BPA's 10-name table mis-defines `green`), and assignment runs exact-first,
# then best-unused, then reuse, instead of BPA's single greedy pass.

_TYPE_MISMATCH_PENALTY = 10000.0
# A type-mismatched spool is still accepted when its colour is this close
# (BPA COLOR_ONLY_MATCH_DISTANCE).
_COLOR_ONLY_MATCH_DISTANCE = 60.0
_TYPE_MATCH_EXCELLENT_DISTANCE = 50.0
_TYPE_MATCH_GOOD_DISTANCE = 150.0


def _match_quality(type_match: bool, dist: float) -> tuple[str, str]:
    """BPA getMatchQuality: (quality key, human label) for one filament/spool pair."""
    if type_match and dist < _TYPE_MATCH_EXCELLENT_DISTANCE:
        return "excellent", "Excellent Match"
    if type_match and dist < _TYPE_MATCH_GOOD_DISTANCE:
        return "good", "Good Match"
    if type_match:
        return "fair", "Type Match"
    if dist < _COLOR_ONLY_MATCH_DISTANCE:
        return "poor", "Color Match Only"
    return "bad", "Poor Match"


def _color_to_rgb(color) -> tuple[int, int, int] | None:
    """Parse '#RRGGBB', '#RRGGBBAA', bare 'RRGGBB', or a CSS3 colour name.

    bpm stores a spool colour as the CSS3 name when webcolors knows the hex
    exactly, else as '#RRGGBBAA'; slicer metadata is '#RRGGBB'. Returns None
    for anything unparseable, which the caller treats as "cannot compare".
    """
    if not isinstance(color, str) or not color.strip():
        return None
    c = color.strip()
    if not c.startswith("#"):
        try:
            import webcolors
            c = webcolors.name_to_hex(c.lower())
        except ValueError:
            c = "#" + c
    h = c.lstrip("#")
    if len(h) == 8:
        h = h[:6]
    if len(h) != 6:
        return None
    try:
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    except ValueError:
        return None


def _color_distance(a, b) -> float:
    """Euclidean RGB distance; inf when either colour cannot be parsed."""
    import math
    ra, rb = _color_to_rgb(a), _color_to_rgb(b)
    if ra is None or rb is None:
        return math.inf
    return math.dist(ra, rb)


def _protocol_tray_id(spool) -> int | None:
    """Wire tray id for an AMS-backed spool (BPA getProtocolTrayId).

    4-slot units (ams_id 0..127): ams_id * 4 + slot_id → 0..103.
    AMS HT / N3S (ams_id 128..253): ams_id + slot_id → 128..135.
    External spool / no AMS: None — not a use_ams candidate.
    """
    try:
        ams_id, slot_id = int(spool.ams_id), int(spool.slot_id)
    except (TypeError, ValueError, AttributeError):
        return None
    if slot_id < 0:
        return None
    if 0 <= ams_id < 128:
        return ams_id * 4 + slot_id
    if 128 <= ams_id < 254:
        return ams_id + slot_id
    return None


def _usable_spools(spools: list) -> list[tuple[int, object]]:
    """(tray_id, spool) for every AMS-backed spool that can be printed from.

    BPA isValidSpool: AMS-backed (external excluded), has a type, and
    state != 0 (an empty slot reports state 0).
    """
    out: list[tuple[int, object]] = []
    for s in spools or []:
        tray = _protocol_tray_id(s)
        if tray is None or not getattr(s, "type", "") or getattr(s, "state", -1) == 0:
            continue
        out.append((tray, s))
    return out


def _resolve_ams_mapping_from_spools(
    filaments: list, spools: list
) -> tuple[list[int], list[dict], list[dict]]:
    """Build the filament-id-indexed wire ams_mapping from the loaded spools.

    Scoring per BPA: exact type match scores 0, mismatch 10000, plus RGB
    colour distance; a pair is acceptable when the type matches or the colour
    alone is within _COLOR_ONLY_MATCH_DISTANCE. Assignment runs three passes
    over the filaments still unmapped:
      1. exact — type matches and the colour distance is 0, unused spool;
      2. best unused — lowest acceptable score among unused spools;
      3. reuse — lowest acceptable score among ALL usable spools, so two
         project filaments of one loaded material share a tray rather than
         refusing the print.
    A filament with no usable id, type, or colour is unmatched. The mapping is
    indexed by 1-based filament id and gap-filled with -1, because the gcode
    inside the 3mf references filaments by id, not list position.

    Returns (wire_mapping, matches, unmatched): one match dict per mapped
    filament (id, type, color, tray_id, spool_type, spool_color, distance,
    quality, label, pass, reused) and the unmatched filament dicts.
    """
    import math
    candidates = _usable_spools(spools)

    def parse_id(f) -> int:
        try:
            return int(f.get("id", 0))
        except (TypeError, ValueError):
            return 0

    entries = [(f, parse_id(f)) for f in (filaments or [])]
    wire = [-1] * max((fid for _, fid in entries), default=0)
    used: set[int] = set()
    matches: list[dict] = []
    unmatched: list[dict] = []
    pending: list[tuple[dict, int]] = []
    for f, fid in entries:
        if fid <= 0 or not str(f.get("type") or "") or not (f.get("color") or ""):
            unmatched.append(f)
        else:
            pending.append((f, fid))

    def scored(f, s) -> tuple[float, bool, float] | None:
        type_match = str(s.type).upper() == str(f.get("type")).upper()
        dist = _color_distance(f.get("color"), getattr(s, "color", ""))
        if dist == math.inf:
            return None
        return (0.0 if type_match else _TYPE_MISMATCH_PENALTY) + dist, type_match, dist

    def acceptable(score: float) -> bool:
        return score < _TYPE_MISMATCH_PENALTY or (
            score - _TYPE_MISMATCH_PENALTY < _COLOR_ONLY_MATCH_DISTANCE
        )

    def assign(f, fid, tray, s, type_match, dist, pass_name, reused):
        quality, label = _match_quality(type_match, dist)
        wire[fid - 1] = tray
        used.add(tray)
        matches.append({
            "id": fid, "type": f.get("type"), "color": f.get("color"),
            "tray_id": tray, "spool_type": s.type, "spool_color": getattr(s, "color", ""),
            "distance": round(dist, 1), "quality": quality, "label": label,
            "pass": pass_name, "reused": reused,
        })

    # Pass 1 — exact type and colour, unused spools only.
    for f, fid in list(pending):
        for tray, s in candidates:
            if tray in used:
                continue
            sc = scored(f, s)
            if sc and sc[1] and sc[2] == 0:
                assign(f, fid, tray, s, True, 0.0, "exact", False)
                pending.remove((f, fid))
                break

    # Pass 2 — best acceptable unused spool; pass 3 — best acceptable spool, reuse allowed.
    for pass_name, allow_reuse in (("best-unused", False), ("reuse", True)):
        for f, fid in list(pending):
            best = None
            for tray, s in candidates:
                if tray in used and not allow_reuse:
                    continue
                sc = scored(f, s)
                if sc and (best is None or sc[0] < best[0]):
                    best = (sc[0], tray, s, sc[1], sc[2])
            if best and acceptable(best[0]):
                _, tray, s, type_match, dist = best
                assign(f, fid, tray, s, type_match, dist, pass_name, tray in used)
                pending.remove((f, fid))

    unmatched.extend(f for f, _ in pending)
    return wire, matches, unmatched


def _resolve_print_mapping(name: str, printer, file_path: str, plate_num: int, info=None) -> dict:
    """Resolve the ams_mapping a print of file_path/plate_num would use, from the spools
    the printer last reported. Never prints. Returns the resolution payload; when the
    print could not proceed on it the payload also carries an "error" message.
    ``info``: project metadata already parsed from verified bytes (print_file); read otherwise.
    """
    import json
    try:
        if info is None:
            from bpm.bambuproject import get_project_info as _get_project_info
            info = _get_project_info(file_path, printer, plate_num=plate_num)
    except Exception as e:
        log.warning("resolve_print_mapping: could not read project metadata for %s: %s", file_path, e)
        return {
            "error": (
                f"Could not read project metadata for {file_path}: {e}. "
                "Pass ams_mapping explicitly or use use_ams=False."
            )
        }
    metadata = getattr(info, "metadata", None) or {}
    filaments = list(metadata.get("filament") or [])
    if info is None or not filaments:
        return {
            "error": (
                f"{file_path} plate {plate_num} carries no filament metadata to map from "
                "(plate missing or unparsed). Pass ams_mapping explicitly or use use_ams=False."
            )
        }
    state = session_manager.get_state(name)
    spools = list(getattr(state, "spools", None) or [])
    wire, matches, unmatched = _resolve_ams_mapping_from_spools(filaments, spools)
    result = {
        "file_path": file_path,
        "plate_num": plate_num,
        "filaments": filaments,
        "resolved_ams_mapping": wire,
        "ams_mapping_json": json.dumps(wire),
        "matches": matches,
        "unmatched": unmatched,
        "loaded_spools": [
            {"tray_id": tray, "type": s.type, "color": getattr(s, "color", "")}
            for tray, s in _usable_spools(spools)
        ],
        "external_spools": [
            {"slot_id": getattr(s, "slot_id", -1), "type": s.type, "color": getattr(s, "color", "")}
            for s in spools if _protocol_tray_id(s) is None and getattr(s, "slot_id", -1) >= 254
        ],
    }
    log.info(
        "resolve_print_mapping: %s plate %s → %s (unmatched=%d)",
        file_path, plate_num, wire, len(unmatched),
    )
    if unmatched or not wire:
        result["error"] = (
            "No loaded AMS spool matches these project filaments: "
            + ", ".join(f"id {f.get('id')} ({f.get('type')} {f.get('color')})" for f in unmatched)
            + ". Load a matching spool, pass ams_mapping explicitly, or use use_ams=False."
        )
    return result


def preview_ams_mapping(name: str, file_path: str, plate_num: int = 1) -> dict:
    """
    Resolve, WITHOUT printing, the ams_mapping that print_file would send for a .3mf plate,
    from the spools the printer last reported loaded.

    WHEN to use: before ``print_file`` when the AMS feeds the print. It returns the exact
    mapping ``print_file`` will send: report each filament's tray_id, the spool it matched, its
    colour distance and the BPA match label (Excellent Match / Good Match / Type Match / Color
    Match Only / Poor Match).

    Sibling disambiguation: ``preview_ams_mapping`` is read-only and never prints;
    ``print_file`` runs the same resolution and then starts the physical print.
    ``get_project_info`` reports the plate's filaments but its ``ams_mapping`` is only a
    filament-id placeholder, and ``get_spool_info`` reports the loaded spools.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        plate_num: Plate to resolve the mapping for (default 1). An absent plate silently
            resolves against the file's first available plate; check ``plates`` from
            ``get_project_info`` first.

    Returns:
        A resolution payload dict: ``file_path``, ``plate_num`` (the value you PASSED, not
        necessarily the plate resolved), ``filaments``,
        ``resolved_ams_mapping`` (list indexed by 1-based filament id, -1 for an unused id),
        ``ams_mapping_json`` (the exact string ``print_file`` sends), ``matches``,
        ``unmatched``, ``loaded_spools`` (the AMS spools that could be printed from) and
        ``external_spools``. When ``"error"`` is present in the payload ``print_file`` would
        refuse with the same message; pass ``ams_mapping`` explicitly or use ``use_ams=False``.
        The payload may then hold only ``{"error": str}``, for a metadata read failure or a plate
        with no filament metadata. If the printer is not connected the result is
        ``{"error": "Printer '<name>' not connected"}``.

    Notes:
        Read-only in purpose: it changes nothing on the printer, though reading the project
        metadata may download the .3mf over FTPS and write the local metadata cache.
        "Type Match" means the material matches but the colour is off: ``print_file`` WILL
        print on it, so surface it to the user. "Color Match Only" means the MATERIAL DOES NOT
        MATCH (for example the project wants PLA and only PETG is loaded) and the spool was
        accepted purely because its colour is close: ``print_file`` WILL print on it too. Call
        this out explicitly for any ``"quality": "poor"`` match; it is a stronger warning than
        a colour mismatch.
    """
    log.debug("preview_ams_mapping: called for name=%s file_path=%s plate_num=%s", name, file_path, plate_num)
    printer = session_manager.get_printer(name)
    if printer is None:
        return _no_printer(name)
    return _resolve_print_mapping(name, printer, file_path, plate_num)


def print_file(
    name: str,
    file_path: str,
    plate_num: int = 1,
    bed_type: str = "auto",
    use_ams: bool = True,
    ams_mapping: list | str | None = None,
    timelapse: bool = False,
    bed_leveling: bool = True,
    flow_calibration: bool = False,
    user_permission: bool = False,
) -> dict:
    """
    Start printing a job that ``prepare_print_job`` prepared and ``upload_file`` uploaded.

    WHEN to use: STEP 3 of a print, after ``prepare_print_job`` and ``upload_file``, when the
    user has asked for this print. That request is the authorization for the task and stays
    valid; it does not need to be repeated per step or parameter. What may start is enforced
    by the server (ADMISSION below), not by a conversational gate.

    WRITE GUARD: starts a physical print on the named printer, which heats, moves and
    extrudes immediately. Once started the job can only be paused (``pause_print``) or
    cancelled (``stop_print``); it cannot be recalled. With ``user_permission`` False the tool
    changes nothing and returns the ``{"error": ...}`` refusal naming that consequence.
    Separately from the guard, the tool is BLOCKED while the printer's reported gcode_state is
    RUNNING or PREPARE (an active-print guard that ``user_permission=True`` cannot override).
    Like every printer write it is also refused unless telemetry is live (a complete, current
    state report on the current connection), so missing or stale state never lets a start
    through.

    ADMISSION: only a remote path that ``upload_file`` bound to a prepared job in this server's
    lifetime can start, once. Immediately before starting, the server downloads the file over
    FTPS and requires the prepared SHA-256, reads the plate metadata from those verified
    bytes, and holds the SD-card lock so this server's uploads, renames and deletes cannot run
    in between. The admission is consumed before the start command is published, so an
    uncertain publish is never repeated: start again only after a new ``upload_file`` (the
    prepared job stays valid; nothing is re-sliced). A file already on the card, a replaced,
    renamed, deleted or modified file, another plate, and anything after a server restart are
    refused before any command is sent. There is no override.
    The printer must also be the job's prepared target (``target`` from ``prepare_print_job``):
    the same model, derived from the serial number the live session answers on (unknown
    refuses; the configured name is never used), one nozzle, and the prepared nozzle diameter
    as the printer reported it on the current connection. The start names the prepared plate
    type. Software cannot sense the plate or filament physically on the printer; that remains
    an observation for the camera or a person.
    STORAGE: the printer unpacks the plate's G-code onto its SD card before printing (several
    times the compressed job). After the card verification the server lists the card again and
    requires room for that G-code plus a 64 MiB reserve, on the same card the job was uploaded
    to (its ``/bambu-mcp-card.json`` record), with an SD state of normal reported on this
    connection (docs/storage-space.md). A refusal keeps the admission (except on another card).

    Sibling disambiguation: ``print_file`` starts the print; ``preview_ams_mapping`` resolves
    the same ams_mapping without printing and is the read-only step before it.
    ``prepare_print_job`` and ``upload_file`` are the two steps that must come first.
    ``get_project_info`` reads the plate's filaments, and ``open_plate_viewer`` shows the
    plates to the human.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: SD card path that ``upload_file`` returned as ``remote_path`` (a leading
            ``/`` is added).
        plate_num: Plate to print (default 1); it must be a plate of the prepared job.
        bed_type: ``auto`` (default) names the plate the job was prepared for (its
            ``target.bed_type``) in the start packet. Naming that same plate is accepted; any
            other of ``cool_plate``, ``eng_plate``, ``hot_plate``, ``textured_plate`` is refused
            with ``bed_type_mismatch`` and an unknown name with ``invalid_bed_type``, both before
            the admission is used (case-insensitive). ``cool_plate`` = smooth cold plate (PLA,
            TPU at low temp); ``eng_plate`` = smooth engineering plate (PETG, PA, ABS);
            ``hot_plate`` = smooth high-temp plate (ASA, PC); ``textured_plate`` = textured PEI
            surface (good general-purpose adhesion).
        use_ams: True (default) loads filament from AMS slots, with the mapping resolved as
            described under ``ams_mapping``. False prints using only the external spool
            holder (single-colour prints without AMS): leave ``ams_mapping`` empty, and bpm
            derives the holder for each filament from the plate's own extruder assignment
            (right holder for a filament sliced for the right extruder, left holder for the
            left) and refuses the print with an error when the plate has no extruder map. The
            holder cannot be chosen.
        ams_mapping: Optional override, default None. When empty (None, ``""`` or ``[]``) and
            ``use_ams`` is True, the mapping is resolved from the spools the printer last
            reported loaded: each project filament is matched to an AMS spool by exact type
            then closest colour (bambu-printer-app's print-dialog scoring; exact pairs are
            assigned first, then best unused, then reuse). A same-material spool of the WRONG
            colour is accepted ("Type Match"). A WRONG-MATERIAL spool is ALSO accepted if its
            colour is close enough ("Color Match Only", no material check at all), so call
            ``preview_ams_mapping`` first and show the user every match label, "Color Match
            Only" especially. The 3mf carries no usable tray ids: ``get_project_info``'s
            ams_mapping is a filament-id placeholder, never a slot assignment. If any filament
            finds no loaded match, or the plate carries no filament metadata, the print is
            REFUSED with the unmatched filaments and the loaded spools listed. To override,
            provide a JSON array string or a list of integers indexed by 1-based filament id
            (index 0 = filament 1), each element an absolute tray_id, -1 for a filament id the
            plate does not use. When ``ams_mapping`` is provided, ``use_ams`` is automatically
            set to True. See Notes for the tray_id encoding.
        timelapse: Record a timelapse (default False).
        bed_leveling: Run bed leveling before printing (default True).
        flow_calibration: Run flow calibration before printing (default False).
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, ...}`` means the print command was PUBLISHED; the printer's
        acceptance and the job's start are not confirmed. Follow up with ``get_print_progress``
        or ``get_job_info``, and ``get_hms_errors`` if nothing starts. The full success shape
        is ``{"success": True, "status": "published_unconfirmed", "file_path": <normalized
        path>, "plate_num": <plate_num>, "bed_type": <resolved bed type name>, "use_ams":
        <bool>, "ams_mapping": <JSON string sent, empty when none>, "matches": <list of
        per-filament match dicts from the live-spool resolution, empty when ``ams_mapping``
        was provided or ``use_ams`` is False>, "admission": {"receipt_id", "part",
        "remote_path", "sha256", "plates", "target", "card_id", "md5", "consumed": True},
        "storage": <space evidence, as for upload_file>}``.
        Errors are ``{"error": str}``: the ``_permission_denied`` refusal when
        ``user_permission`` is False, ``"Printer '<name>' not connected"``, the active-print
        block ``"Blocked: '<name>' is currently <state>. ..."``, or
        ``"Error starting print: <exception>"`` (then ``"admission": "consumed"``: upload
        again before another attempt). Admission refusals, all sent before any command, are
        ``{"error": code, "detail": str}`` with code ``print_not_admitted``,
        ``plate_not_admitted``, ``remote_file_missing``, ``remote_file_mismatch``,
        ``remote_verification_failed`` (FTPS error; the admission is kept), ``invalid_bed_type``,
        ``bed_type_mismatch``, ``printer_model_unknown``, ``printer_model_mismatch``,
        ``printer_not_qualified``, ``nozzle_unknown``, ``nozzle_mismatch``,
        ``start_md5_unavailable``, ``storage_state_unknown``, ``storage_no_card``,
        ``storage_card_abnormal``, ``storage_card_read_only``, ``storage_unattested``,
        ``storage_unknown``, ``storage_insufficient`` (with ``storage``: available, required and
        their basis), ``storage_card_changed`` (admission dropped), ``invalid_ams_mapping``, ``print_metadata_unavailable``, ``filament_change_in_progress``, ``invalid_remote_path``
        or ``file_operation_busy``. When the live-spool resolution fails, the return is the
        resolution payload of ``preview_ams_mapping`` (``file_path``, ``plate_num``,
        ``filaments``, ``resolved_ams_mapping``, ``ams_mapping_json``, ``matches``,
        ``unmatched``, ``loaded_spools``, ``external_spools``) plus an ``"error"`` message, or
        only ``{"error": str}`` when the project metadata could not be read; the admission is
        kept for those.

    Notes:
        Calls ``printer.print_3mf_file()`` with the given parameters. The MQTT publish guard
        lets that one ``project_file`` packet through only when its file, plate, bed type,
        options and md5 (exactly the MD5 of the verified bytes) match what this call verified;
        any other start packet, from any tool or route, is refused there.

        tray_id encoding for ``ams_mapping``: ALWAYS derive from live telemetry (the spool's
        ams_id is the hardware chip_id from ``get_ams_units`` / ``get_spool_info``), NEVER
        hardcode:
          4-slot AMS (ams_id 0..127):  tray_id = ams_id * 4 + slot_id   -> 0..103
          AMS HT / N3S (ams_id >= 128): tray_id = ams_id + slot_id      -> 128..
          Unused filament id = -1. External spool: not part of this array, use use_ams=False.
        NEVER use the 0-based unit_index in place of ams_id, and NEVER apply the 4-slot formula
        to an AMS HT (ams_id 128 -> 512 is wrong; 128 is right).

        Correct workflow when overriding:
          1. Call ``preview_ams_mapping`` to see what the tool would resolve, then
             ``get_spool_info`` for each spool's ams_id and slot_id.
          2. Encode each chosen spool with the formula above.
          3. Build the array indexed by 1-based filament id from ``get_project_info``.

        Example: if AMS 2 Pro has ams_id=0, slot 1 -> tray_id=1.
                 if AMS HT has ams_id=128, slot 0 -> tray_id=128.
        Always call ``get_project_info`` first to see which filaments the .3mf plate uses.

        Workflow: ``prepare_print_job(part)`` -> ``upload_file`` with its ``job_path`` ->
        ``print_file`` on the returned ``remote_path``. When the AMS feeds the print,
        ``preview_ams_mapping`` returns the exact mapping this tool sends; report its match
        labels (Type Match and Color Match Only especially, since software cannot sense the
        loaded material). To show the plate to a person use ``open_plate_viewer``, never raw
        thumbnails. For ``bed_leveling``, ``flow_calibration`` and ``timelapse`` use what the
        user asked for, else their stored preference (``get_user_pref``; null means the
        factory default: bed_leveling=True, flow_calibration=False, timelapse=False), and
        store the values used with ``set_user_pref`` after a successful start.
    """
    log.debug("print_file: called for name=%s file_path=%s plate_num=%s bed_type=%s user_permission=%s", name, file_path, plate_num, bed_type, user_permission)
    if not user_permission:
        log.debug("print_file: permission denied for %s", name)
        return {"error": _permission_denied(
            "This would start a physical print on the printer, which heats, moves and extrudes and cannot be undone."
        )}
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("print_file: printer not connected: %s", name)
        return _no_printer(name)
    from tools._guards import check_active_print_guard
    blocked = check_active_print_guard(printer, name, "print_file")
    if blocked:
        return blocked
    import print_admission
    try:
        path = print_admission.normalize_remote(file_path)
        with print_admission.file_lock(name):
            return _start_admitted(name, printer, path, plate_num, bed_type, use_ams, ams_mapping,
                                   timelapse, bed_leveling, flow_calibration)
    except print_admission.Refused as r:
        log.warning("print_file: refused for %s: %s", name, r)
        return r.as_dict()
    except Exception as e:
        # Only steps before the admission is consumed can raise out of _start_admitted.
        log.error("print_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error starting print: {e}", "detail": "No start command was sent."}


def _start_admitted(name, printer, path, plate_num, bed_type, use_ams, ams_mapping, timelapse,
                    bed_leveling, flow_calibration) -> dict:
    """print_file under the SD-card lock: verify, consume the admission, publish once."""
    import ftplib
    import json
    import shutil
    import print_admission as pa
    import storage_space
    from bpm.bambuproject import cached_project_md5, get_project_info as _read_project
    from bpm.bambutools import PlateType
    from tools._guards import check_active_print_guard

    admitted = pa.binding(name, path)
    if admitted is None:
        raise pa.Refused("print_not_admitted",
                         f"{path} is not a prepared job uploaded by upload_file in this server's lifetime, or its "
                         "admission was already used, replaced, renamed, deleted or lost in a server restart. "
                         f"No command was sent. {pa._NEXT_STEP}")
    if plate_num not in admitted.plates:
        raise pa.Refused("plate_not_admitted", f"Plate {plate_num} is not a plate of the prepared job "
                         f"(plates {list(admitted.plates)}). No command was sent.")
    if bed_type and bed_type.upper() not in PlateType.__members__:
        return {"error": "invalid_bed_type", "detail": bed_type}
    # The start names the prepared plate ('auto' resolves to it; another plate is refused).
    bed_enum = PlateType[pa.start_plate(admitted, bed_type)]
    pa.check_printer(printer, admitted)
    try:
        sha, md5, _, verified, tmpdir = pa.remote_digest(printer, path)
    except ftplib.error_perm as exc:
        pa.drop(name, path, "missing on the card")
        raise pa.Refused("remote_file_missing", f"{path} could not be read from the card ({exc}); its admission is "
                         f"dropped. No command was sent. {pa._NEXT_STEP}") from None
    except Exception as exc:
        raise pa.Refused("remote_verification_failed",
                         f"{path} could not be downloaded to verify it ({type(exc).__name__}: {exc}). No command was "
                         "sent; the admission is kept, so print_file may be called again.") from None
    try:
        if sha != admitted.sha256:
            pa.drop(name, path, "remote bytes differ from the prepared job")
            raise pa.Refused("remote_file_mismatch",
                             f"{path} on the card is not the prepared job (SHA-256 {sha}, expected "
                             f"{admitted.sha256}); its admission is dropped. No command was sent. {pa._NEXT_STEP}")
        # Plate metadata (filaments, extruders, md5) from the verified bytes only, never the cache.
        project = _read_project(path, printer, plate_num=plate_num, local_file=verified, use_cached_list=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    if project is None or project.plate_num != plate_num or not project.metadata.get("filament"):
        return {"error": "print_metadata_unavailable", "detail": "The requested plate and its filament must be verified before printing; no start command was sent."}
    # bambu puts its cached md5 for this path into the start packet; it must be the verified bytes'.
    if cached_project_md5(printer, path, plate_num) != md5:
        raise pa.Refused("start_md5_unavailable",
                         f"bambu's metadata for {path} does not carry the verified MD5 {md5}, so the start packet "
                         "could not name the verified file. No command was sent; the admission is kept.")
    # The printer unpacks the plate's G-code to the card before printing; free space may have
    # changed since the upload. A refusal keeps the admission, except on another card.
    try:
        storage = storage_space.require_for_start(name, printer, admitted, plate_num)
    except pa.Refused as r:
        if r.code == "storage_card_changed":
            pa.drop(name, path, "the SD card was replaced")
            r.detail += " Its admission is dropped; upload the prepared job again."
        else:
            r.detail += " The admission is kept, so print_file may be called again."
        raise
    matches: list[dict] = []
    # "" and [] are the empty forms clients send for "no mapping" (BPA's own URL
    # sends ams_mapping=); treat them as absent rather than as an empty mapping.
    if ams_mapping not in (None, "", []):
        # Coerce list → JSON string so print_3mf_file's json.loads() works
        if isinstance(ams_mapping, list):
            ams_mapping = json.dumps(ams_mapping)
        try:
            ticket_mapping = tuple(int(x) for x in json.loads(ams_mapping))
        except (TypeError, ValueError):
            return {"error": "invalid_ams_mapping", "detail": f"{ams_mapping!r} is not a JSON array of tray ids; no start command was sent."}
        resolved_ams_mapping = ams_mapping
        use_ams = True
        log.debug("print_file: using caller-provided ams_mapping: %s", ams_mapping)
    else:
        resolved_ams_mapping, ticket_mapping = "", None
        if use_ams:
            res = _resolve_print_mapping(name, printer, path, plate_num, info=project)
            if "error" in res:
                log.warning("print_file: refused for %s: %s", name, res["error"])
                return res
            resolved_ams_mapping = res["ams_mapping_json"]
            ticket_mapping = tuple(res["resolved_ams_mapping"])
            matches = res["matches"]
    # Metadata/FTPS can take seconds. Recheck immediately before publishing.
    blocked = check_active_print_guard(printer, name, "print_file")
    if blocked:
        return blocked
    tray = getattr(printer.printer_state, "active_tray_state", "")
    if getattr(tray, "name", str(tray)).upper() in ("LOADING", "UNLOADING"):
        return {"error": "filament_change_in_progress", "detail": "Finish the filament change before starting a print."}
    pa.check_printer(printer, admitted)  # again: the nozzle report may have changed during verification
    storage_space.check_sd_state(printer)  # and the card may have been removed
    ticket = pa.StartTicket(path=path, plate=plate_num, use_ams=bool(use_ams), ams_mapping=ticket_mapping,
                            bed_type=bed_enum.name.lower(), bed_leveling=bool(bed_leveling),
                            flow=bool(flow_calibration), timelapse=bool(timelapse), md5=md5)
    if not pa.consume(name, admitted):
        raise pa.Refused("print_not_admitted", f"The admission for {path} changed during verification. No command was sent.")
    admission = {**admitted.public(), "md5": md5, "consumed": True}
    log.debug("print_file: calling printer.print_3mf_file for %s", name)
    try:
        with pa.start_ticket(ticket):
            printer.print_3mf_file(
                name=path,
                plate=plate_num,
                bed=bed_enum,
                use_ams=use_ams,
                ams_mapping=resolved_ams_mapping,
                bedlevel=bed_leveling,
                flow=flow_calibration,
                timelapse=timelapse,
            )
    except Exception as e:
        log.error("print_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error starting print: {e}", "admission": "consumed",
                "detail": "The admission was used for this attempt and is not retried. Check get_print_progress "
                          "before anything else; to try again, run upload_file with the prepared job_path first."}
    log.debug("print_file: → file=%s plate=%s bed_type=%s use_ams=%s ams_mapping=%s", path, plate_num, bed_enum.name, use_ams, resolved_ams_mapping)
    return {
        "success": True,
        "status": "published_unconfirmed",
        "file_path": path,
        "plate_num": plate_num,
        "bed_type": bed_enum.name,
        "use_ams": use_ams,
        "ams_mapping": resolved_ams_mapping,
        "matches": matches,
        "admission": admission,
        "storage": storage,
    }


def _build_layout_uri(topimg_uri: str, objs: list, model_key) -> str:
    """Render bounding-box overlay on topimg and return a PNG data URI, or '' on failure."""
    log.debug("_build_layout_uri: called with topimg length=%d, obj count=%d", len(topimg_uri), len(objs))
    try:
        import base64
        import io
        from PIL import Image, ImageDraw, ImageFont

        img = Image.open(
            io.BytesIO(base64.b64decode(topimg_uri.split(",", 1)[1]))
        ).convert("RGBA")
        W, H = img.size

        model_str = model_key.value if hasattr(model_key, "value") else str(model_key).lower()
        BED_W, BED_H = _BED_DIMENSIONS.get(model_str, (256.0, 256.0))
        scale = min(W / BED_W, H / BED_H)
        x_off = (W - BED_W * scale) / 2
        y_off = (H - BED_H * scale) / 2

        def mm_to_px(x_mm, y_mm):
            return (x_off + x_mm * scale, H - y_off - y_mm * scale)

        unique_names = list(dict.fromkeys(o["name"] for o in objs))
        color_map = {n: _BBOX_PALETTE[i % len(_BBOX_PALETTE)] for i, n in enumerate(unique_names)}

        # Draw largest bboxes first so smaller parts aren't obscured
        objs_sorted = sorted(
            objs,
            key=lambda o: (o["bbox"][2] - o["bbox"][0]) * (o["bbox"][3] - o["bbox"][1]),
            reverse=True,
        )
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        for o in objs_sorted:
            bx0, by0, bx1, by1 = o["bbox"]
            px0, py1 = mm_to_px(bx0, by0)
            px1, py0 = mm_to_px(bx1, by1)
            c = color_map[o["name"]]
            draw.rectangle([px0, py0, px1, py1], outline=(*c, 230), width=2, fill=(*c, 40))

        out = Image.new("RGBA", img.size, (0, 0, 0, 0))
        out.paste(img, (0, 0))
        out.paste(overlay, (0, 0), overlay)

        leg_row = 28
        legend = Image.new("RGBA", (W, H + leg_row * len(unique_names) + 20), (30, 30, 30, 255))
        legend.paste(out, (0, 0))
        ld = ImageDraw.Draw(legend)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
        except Exception:
            font = ImageFont.load_default()
        y_pos = H + 10
        for n in unique_names:
            c = color_map[n]
            ld.rectangle([10, y_pos, 26, y_pos + 16], fill=(*c, 220))
            ld.text((34, y_pos), n.replace(".stl", ""), fill=(220, 220, 220, 255), font=font)
            y_pos += leg_row

        buf = io.BytesIO()
        legend.convert("RGB").save(buf, format="PNG")
        result = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
        log.debug("_build_layout_uri: → png data uri, %d bytes raw", len(buf.getvalue()))
        return result
    except Exception:
        log.debug("_build_layout_uri: → empty (exception)")
        return ""


def open_plate_viewer(name: str, file_path: str, target_plate: int = None) -> dict:
    """
    Build and open an HTML viewer of the isometric and top-down images for the plates of a
    3MF project file on the printer's SD card.

    WHEN to use: the human should see the plates, for example to visually confirm which plate
    to print before calling ``print_file``, or to jump to the plate a finished job printed.

    Sibling disambiguation: ``open_plate_viewer`` shows every plate the file really contains
    (isometric, top-down and, when objects are known, a layout image per plate) in a browser
    page, headed with each plate's own number even when the numbers are sparse;
    ``open_plate_layout`` shows one plate as a single annotated top-down PNG. The
    ``get_plate_thumbnail`` and ``get_plate_topview`` tools return raw image data for the AI
    agent rather than opening anything for the human.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        target_plate: Optional plate number to scroll straight to on open. If set, the browser
            opens with the URL fragment ``#plate-{target_plate}``. Useful after a job
            completes: pass the plate number from ``get_job_info`` to jump to the printed plate.
            The anchors are the file's real plate numbers, so a number the file does not
            contain scrolls nowhere.

    Returns:
        ``{"success": True, "path": <path of the HTML file written under /tmp>, "plates":
        <number of plates shown>}`` on success. Errors are ``{"error": str}``:
        ``"Printer '<name>' not connected"``, ``"Could not retrieve project info for '<file_path>'"``, or
        ``"Error building plate viewer: <exception>"``.

    Notes:
        Fetches project info for the plates via the local cache (the .3mf may be downloaded
        over FTPS and the cache written on a miss), embeds the base64 images directly in the
        HTML, writes it to ``/tmp/plate_viewer_<name>.html`` (overwriting any earlier viewer
        for that printer), and opens it in the default browser. The plate set is the one
        ``get_all_project_info`` returns: plate numbers are not assumed to be contiguous or to
        start at 1, so a file with plates [1,5,6,12] shows exactly those four, each under its
        own number. Only plates that genuinely exist are fetched, and the library's ceiling of
        30 applies: a plate numbered above 30 is not shown. The .3mf is downloaded at most
        once, and the listing of the SD card is refreshed once for the whole batch.
    """
    log.debug("open_plate_viewer: called for name=%s file_path=%s", name, file_path)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("open_plate_viewer: printer not connected: %s", name)
        return _no_printer(name)
    try:
        import webbrowser
        from bpm.bambuproject import get_all_project_info as _get_all_project_info

        model_key = getattr(getattr(printer, "config", None), "printer_model", None)

        # The same plate-set lookup the get_all_project_info tool uses: only the plates the
        # .3mf really contains, sparse numbers included, each carrying its own plate_num.
        infos = _get_all_project_info(file_path, printer)
        if not infos:
            log.debug("open_plate_viewer: → error: no project info for %s", file_path)
            return {"error": f"Could not retrieve project info for '{file_path}'"}
        total_plates = len(infos)

        plates_html = ""
        for info in infos:
            p = info.plate_num
            d = _serialize_project_info(info, include_images=True)
            meta = d.get("metadata", {})
            topimg = meta.get("topimg", "")
            thumbnail = meta.get("thumbnail", "")
            objs = [
                o for o in meta.get("map", {}).get("bbox_objects", [])
                if "wipe_tower" not in o.get("name", "")
            ]
            label = ", ".join(o["name"].replace(".stl", "") for o in objs)
            layout_uri = _build_layout_uri(topimg, objs, model_key) if objs and topimg else ""
            layout_html = (
                f'<div class="imgbox"><div class="imglabel">Layout</div><img src="{layout_uri}"></div>'
                if layout_uri else ""
            )
            plates_html += (
                f'<div class="plate" id="plate-{p}"><h3>Plate {p}</h3>'
                f'<p class="parts">{label}</p>'
                f'<div class="imgs">'
                f'<div class="imgbox"><div class="imglabel">Isometric</div><img src="{thumbnail}"></div>'
                f'<div class="imgbox"><div class="imglabel">Top Down</div><img src="{topimg}"></div>'
                f'{layout_html}'
                f'</div></div>\n'
            )

        title = file_path.rsplit("/", 1)[-1].replace(".gcode.3mf", "").replace(".3mf", "")
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>
body{{font-family:sans-serif;background:#111;color:#eee;padding:20px;margin:0}}
h1{{text-align:center;margin-bottom:24px}}
.grid{{display:flex;flex-wrap:wrap;gap:16px;justify-content:center}}
.plate{{background:#222;border-radius:8px;padding:12px;text-align:center;scroll-margin-top:16px}}
.plate h3{{margin:0 0 4px;font-size:1.1em}}
.parts{{font-size:.75em;color:#aaa;margin:0 0 8px;min-height:1em}}
.imgs{{display:flex;gap:10px;justify-content:center;flex-wrap:wrap}}
.imgbox{{display:flex;flex-direction:column;align-items:center}}
.imglabel{{font-size:.7em;color:#888;margin-bottom:4px}}
.plate img{{width:300px;height:300px;object-fit:contain;display:block}}
</style></head>
<body><h1>{title}</h1>
<div class="grid">{plates_html}</div></body></html>"""

        out_path = f"/tmp/plate_viewer_{name}.html"
        with open(out_path, "w") as f:
            f.write(html)

        fragment = f"#plate-{target_plate}" if target_plate else ""
        webbrowser.open(f"file://{out_path}{fragment}")
        log.info("open_plate_viewer: opened viewer for %s, plates=%d", name, total_plates)
        log.debug("open_plate_viewer: → path=%s plates=%d", out_path, total_plates)
        return {"success": True, "path": out_path, "plates": total_plates}
    except Exception as e:
        log.error("open_plate_viewer: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error building plate viewer: {e}"}


# Bed dimensions (W x H in mm) keyed by PrinterModel.value
_BED_DIMENSIONS: dict[str, tuple[float, float]] = {
    "h2d":    (350.0, 320.0),
    "h2s":    (350.0, 320.0),
    "x1c":    (256.0, 256.0),
    "x1":     (256.0, 256.0),
    "x1e":    (256.0, 256.0),
    "p1s":    (256.0, 256.0),
    "p1p":    (256.0, 256.0),
    "p2s":    (256.0, 256.0),
    "a1":     (256.0, 256.0),
    "a1_mini": (180.0, 180.0),
}

_BBOX_PALETTE = [
    (255, 80,  80),
    ( 80, 200,  80),
    ( 80, 140, 255),
    (255, 200,   0),
    (255, 100, 255),
    (  0, 220, 220),
    (255, 160,  50),
    (180, 255, 100),
]


def open_plate_layout(name: str, file_path: str, plate_num: int = 1) -> dict:
    """
    Generate and open an annotated top-down image for a single plate, with each object's
    bounding box overlaid on the top-view image.

    WHEN to use: the human should see where each part sits on one plate's build surface, with
    a colour legend of part names, for example to confirm object placement before printing.

    Sibling disambiguation: ``open_plate_layout`` produces one annotated PNG for a single
    plate; ``open_plate_viewer`` opens an HTML page showing all plates of the file.
    ``get_plate_topview`` returns the plain top-down image data for the AI agent without
    annotation or opening anything.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        file_path: Full SD card path of the .3mf file.
        plate_num: Plate to render (default 1). An absent plate silently renders another
            available plate; check ``plates`` from ``get_project_info`` first.

    Returns:
        ``{"success": True, "path": <PNG path under /tmp>, "plate": <the plate_num you
        PASSED, not necessarily the plate rendered>, "objects": <bounding-box object count>,
        "unique_parts": <distinct part name count>, "bed_mm": "<W>x<H>"}`` on success.
        ``objects`` and ``unique_parts`` COUNT wipe-tower entries, and the wipe tower is drawn
        on the image and listed in the legend; unlike ``open_plate_viewer`` this tool applies
        no wipe_tower filter, so subtract entries whose name contains ``wipe_tower`` when
        reporting a part count. Errors are ``{"error": str}``: ``"Printer '<name>' not
        connected"``, ``"Could not retrieve project info for plate <plate_num>"``,
        ``"No top-down image available for this plate"``, ``"No bounding-box objects found for
        this plate"``, or ``"Error building plate layout: <exception>"``.

    Notes:
        The PNG is saved to ``/tmp/plate_layout_<name>_p<plate_num>.png`` (overwriting any
        earlier one) and opened in the default viewer. Reading the project info may download
        the .3mf over FTPS and write the local metadata cache.

        Bed dimensions are selected by printer model (mm, W x H): H2D/H2S=350x320,
        X1C/X1/X1E/P1S/P1P/P2S/A1=256x256, A1_MINI=180x180.

        Coordinate mapping applied internally:
        - Slicer bbox coordinates use a bottom-left origin (mm); the image uses a top-left origin.
        - scale = min(img_w / bed_w, img_h / bed_h): uniform scale, no distortion.
        - x_off = (img_w - bed_w * scale) / 2; y_off = (img_h - bed_h * scale) / 2: centring.
        - pixel_x = x_off + x_mm * scale; pixel_y = img_h - y_off - y_mm * scale: Y flip.

        Each unique part name is assigned a distinct colour; a legend with part names and
        colours is appended below the annotated image.
    """
    log.debug("open_plate_layout: called for name=%s file_path=%s plate_num=%s", name, file_path, plate_num)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("open_plate_layout: printer not connected: %s", name)
        return _no_printer(name)
    try:
        import base64
        import dataclasses
        import io
        import json
        import webbrowser
        from enum import Enum

        from bpm.bambuproject import get_project_info as _get_project_info
        from PIL import Image, ImageDraw, ImageFont

        def _to_dict(o):
            if isinstance(o, Enum):
                return o.name
            if dataclasses.is_dataclass(o) and not isinstance(o, type):
                return {f.name: _to_dict(getattr(o, f.name)) for f in dataclasses.fields(o)}
            if isinstance(o, dict):
                return {k: _to_dict(v) for k, v in o.items()}
            if isinstance(o, (list, tuple)):
                return [_to_dict(v) for v in o]
            return o

        info = _get_project_info(file_path, printer, plate_num=plate_num)
        if info is None:
            log.debug("open_plate_layout: → error: no info for %s plate=%s", file_path, plate_num)
            return {"error": f"Could not retrieve project info for plate {plate_num}"}
        d = json.loads(json.dumps(_to_dict(info), default=str))

        meta = d.get("metadata", {})
        topimg_uri = meta.get("topimg", "")
        if not topimg_uri:
            log.debug("open_plate_layout: → error: no topimg for %s plate=%s", file_path, plate_num)
            return {"error": "No top-down image available for this plate"}

        img = Image.open(
            io.BytesIO(base64.b64decode(topimg_uri.split(",", 1)[1]))
        ).convert("RGBA")
        W, H = img.size

        # Bed dimensions from printer model
        model_key = getattr(getattr(printer, "config", None), "printer_model", None)
        model_str = model_key.value if hasattr(model_key, "value") else str(model_key).lower()
        BED_W, BED_H = _BED_DIMENSIONS.get(model_str, (256.0, 256.0))

        # Uniform scale preserving aspect ratio, centred in the image canvas
        scale = min(W / BED_W, H / BED_H)
        x_off = (W - BED_W * scale) / 2
        y_off = (H - BED_H * scale) / 2  # padding at top/bottom (or left/right)

        def mm_to_px(x_mm: float, y_mm: float) -> tuple[float, float]:
            # Slicer origin is bottom-left; image origin is top-left — flip Y
            return (
                x_off + x_mm * scale,
                H - y_off - y_mm * scale,
            )

        objs = meta.get("map", {}).get("bbox_objects", [])
        if not objs:
            log.debug("open_plate_layout: → error: no bbox objects for %s plate=%s", file_path, plate_num)
            return {"error": "No bounding-box objects found for this plate"}

        unique_names = list(dict.fromkeys(o["name"] for o in objs))
        color_map = {
            n: _BBOX_PALETTE[i % len(_BBOX_PALETTE)]
            for i, n in enumerate(unique_names)
        }

        # Draw largest bboxes first so smaller parts aren't obscured
        objs_sorted = sorted(
            objs,
            key=lambda o: (o["bbox"][2] - o["bbox"][0]) * (o["bbox"][3] - o["bbox"][1]),
            reverse=True,
        )
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        for o in objs_sorted:
            bx0, by0, bx1, by1 = o["bbox"]
            px0, py1 = mm_to_px(bx0, by0)   # low y_mm  → high pixel y
            px1, py0 = mm_to_px(bx1, by1)   # high y_mm → low pixel y
            c = color_map[o["name"]]
            draw.rectangle([px0, py0, px1, py1], outline=(*c, 230), width=2, fill=(*c, 40))

        leg_row = 28
        legend_h = leg_row * len(unique_names) + 20
        out_img = Image.new("RGBA", (W, H + legend_h), (30, 30, 30, 255))
        out_img.paste(img, (0, 0))
        out_img.paste(overlay, (0, 0), overlay)
        ld = ImageDraw.Draw(out_img)
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
        except Exception:
            font = ImageFont.load_default()
        y_pos = H + 10
        for n in unique_names:
            c = color_map[n]
            ld.rectangle([10, y_pos, 26, y_pos + 16], fill=(*c, 220))
            ld.text((34, y_pos), n.replace(".stl", ""), fill=(220, 220, 220, 255), font=font)
            y_pos += leg_row

        title = file_path.rsplit("/", 1)[-1].replace(".gcode.3mf", "").replace(".3mf", "")
        out_path = f"/tmp/plate_layout_{name}_p{plate_num}.png"
        out_img.convert("RGB").save(out_path)

        webbrowser.open(f"file://{out_path}")
        log.info("open_plate_layout: opened layout for %s plate %s", name, plate_num)
        log.debug("open_plate_layout: → path=%s objects=%d unique_parts=%d", out_path, len(objs), len(unique_names))
        return {
            "success": True,
            "path": out_path,
            "plate": plate_num,
            "objects": len(objs),
            "unique_parts": len(unique_names),
            "bed_mm": f"{BED_W}x{BED_H}",
        }
    except Exception as e:
        log.error("open_plate_layout: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error building plate layout: {e}"}


def rename_sdcard_file(
    name: str,
    src_path: str,
    dest_path: str,
    user_permission: bool = False,
) -> dict:
    """
    Rename or move a file on the printer's SD card.

    WHEN to use: change a file's name, or move it to another folder on the card, without
    re-uploading it.

    WRITE GUARD: renames or moves the file on the printer's SD card over FTPS, so it no longer
    exists at its old path. With ``user_permission`` False the tool changes nothing and returns
    the ``{"error": ...}`` refusal naming that consequence.

    Sibling disambiguation: ``rename_sdcard_file`` moves or renames a file that is already on
    the card; ``upload_file`` copies a new file from this host, and ``delete_file`` removes a
    file's data.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        src_path: Full SD card path of the existing file, for example
            ``'/cache/my_old_name.gcode.3mf'``.
        dest_path: Full SD card path to move it to. Both paths must be on the SD card.
        user_permission: Must be True to execute. Default False.

    Returns:
        ``{"success": True, "src_path": <normalized src>, "dest_path": <normalized dest>}`` on
        success (no listing is returned). Errors are ``{"error": str}``: the
        ``_permission_denied`` refusal when ``user_permission`` is False, ``"Printer '<name>'
        not connected"``, or ``"Error renaming file on '<name>': <exception>"``; or
        ``{"error": code, "detail": str}`` with code ``rename_creates_job`` (a non-printable
        name renamed to a ``.3mf``/``.gcode`` name), ``storage_record_protected`` (destination is
        the SD card record), ``invalid_remote_path`` or
        ``file_operation_busy``.

    Notes:
        This is an FTPS rename operation, not a copy: the file is moved or renamed in place and
        no data is re-uploaded. Each call then performs a FULL live FTPS listing of the card
        and repopulates the printer library's cached trees; a listing that raises surfaces as a
        rename error even though the rename succeeded, while a listing the library reports as
        failed is not surfaced, because the rename it followed did succeed. Cached plate
        metadata keyed to the old path is not renamed.
        Print admissions (see ``upload_file``) for the source, the destination and anything
        under either as a folder are dropped before the rename: a renamed or overwritten job
        must be uploaded again before ``print_file`` will start it. Renaming a file whose name
        is not printable to a printable name is refused, because that would put an unprepared
        job on the card.
    """
    log.debug("rename_sdcard_file: called for name=%s src=%s dest=%s user_permission=%s", name, src_path, dest_path, user_permission)
    if not user_permission:
        return {"error": _permission_denied(
            "This would rename or move the file on the printer's SD card, so it would no longer exist at its old path."
        )}
    printer = session_manager.get_printer(name)
    if printer is None:
        return _no_printer(name)
    import print_admission
    try:
        src, dest, _ = _guarded_rename(name, printer, src_path, dest_path)
        log.debug("rename_sdcard_file: renamed %s → %s on %s", src, dest, name)
        return {"success": True, "src_path": src, "dest_path": dest}
    except print_admission.Refused as r:
        return r.as_dict()
    except Exception as e:
        log.error("rename_sdcard_file: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error renaming file on '{name}': {e}"}


def _guarded_rename(name: str, printer, src_path: str, dest_path: str):
    """Rename on the card, dropping admissions it invalidates first. Returns (src, dest, tree).

    Shared with PATCH /api/rename_sdcard_file. Raises print_admission.Refused.
    """
    import print_admission as pa
    import storage_space
    src, dest = pa.normalize_remote(src_path), pa.normalize_remote(dest_path)
    storage_space.protect_record(dest)
    if pa.is_job_name(dest) and not pa.is_job_name(src):
        raise pa.Refused("rename_creates_job",
                         f"Renaming {src} to the printable name {dest} would create an unprepared job on the card. "
                         "Nothing was renamed. Upload a job prepared by prepare_print_job instead.")
    with pa.file_lock(name):
        for path in dict.fromkeys((src, dest, src.rstrip("/") + "/", dest.rstrip("/") + "/")):
            pa.drop(name, path, f"renamed {src} -> {dest}")
        return src, dest, printer.rename_sdcard_file(src, dest)


def get_current_job_project_info(name: str, include_images: bool = False) -> dict:
    """
    Return 3MF project properties for the currently active print job.

    WHEN to use: intended to find out which plate and parts the running (or just-finished) job
    is printing without knowing the file path in advance. CURRENTLY IT ALWAYS RETURNS THE
    ``no_active_job`` ERROR (see Returns), so use ``get_job_info`` plus
    ``get_3mf_entry_by_name`` and ``get_project_info`` instead, or the HTTP route
    ``GET /api/get_current_3mf_props``.

    Sibling disambiguation: ``get_current_job_project_info`` is meant to read the active job's
    gcode_file and plate from the printer's live job state and return what ``get_project_info``
    returns for them. ``get_project_info`` needs you to supply the file path and plate;
    ``get_job_info`` returns the job's own record (subtask name, gcode file, plate, layer
    counts, times), not the project's metadata.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        include_images: True embeds the base64 thumbnail and top-view data URIs in the response
            (large). Default False. See ``get_project_info`` for the full field documentation.

    Returns:
        In practice ``{"error": "no_active_job", "gcode_state": "", "note": "No print is
        currently running or paused."}`` on every call, even mid-print: the tool reads
        ``gcode_state`` from the job record, which has no such field, so the state is always
        empty and the no-job branch is always taken. The other errors are ``{"error": str}``:
        ``"Printer '<name>' not connected"`` and ``"Error retrieving current job project info
        for '<name>': <exception>"``. The intended (currently unreachable) success value is the
        ``get_project_info`` result for the active job's file and plate plus a ``"gcode_state"``
        key.

    Notes:
        Code defect for the owner: ``gcode_state`` should come from the printer state, the .3mf
        should be resolved from the job's ``subtask_name`` (``<subtask_name>.gcode.3mf``) or
        its ``project_info`` rather than from ``gcode_file`` (which is the printer-internal
        ``/data/Metadata/plate_N.gcode`` path, not an SD card path), and a ``plate_num`` of -1
        means unknown. When ``include_images=True`` the intended response carries raw base64
        data URIs which may exceed the CLI inline display limit. The HTTP route
        ``GET http://localhost:{api_port}/api/get_current_3mf_props?printer={name}`` returns the
        active job's cached project info. Call ``kb_get('bambu-http-files')`` for full route
        docs. Pre-authorized, no human permission needed.
    """
    log.debug("get_current_job_project_info: called for name=%s include_images=%s", name, include_images)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("get_current_job_project_info: printer not connected: %s", name)
        return _no_printer(name)
    try:
        job = printer.active_job_info
        gcode_state = getattr(job, "gcode_state", None) or ""
        gcode_file = getattr(job, "gcode_file", None) or ""
        plate_num = getattr(job, "plate_num", 1) or 1
        log.debug("get_current_job_project_info: gcode_state=%s gcode_file=%s plate_num=%s", gcode_state, gcode_file, plate_num)
        if not gcode_file or gcode_state.upper() in ("IDLE", ""):
            log.debug("get_current_job_project_info: no active job for %s (state=%s file=%s)", name, gcode_state, gcode_file)
            return {"error": "no_active_job", "gcode_state": gcode_state, "note": "No print is currently running or paused."}
        log.debug("get_current_job_project_info: delegating to get_project_info for %s file=%s plate=%s", name, gcode_file, plate_num)
        result = get_project_info(name, gcode_file, plate_num=plate_num, include_images=include_images)
        result["gcode_state"] = gcode_state
        return result
    except Exception as e:
        log.error("get_current_job_project_info: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error retrieving current job project info for '{name}': {e}"}


def refresh_sdcard(name: str, mode: str = "full") -> dict:
    """
    Force a fresh SD card listing from the printer.

    WHEN to use: you need up-to-date cached SD card data, for example after uploading a file or
    after a print completes, before reading it with ``list_sdcard_files(cached=True)``.

    Sibling disambiguation: ``refresh_sdcard`` re-reads the card and returns no listing;
    ``list_sdcard_files`` returns the listing itself, and with its default ``cached=False`` it
    already does a live FTPS fetch of its own.

    Args:
        name: Configured printer name (see ``get_configured_printers``).
        mode: ``'full'`` (default) and ``'3mf'`` both perform ONE full live FTPS listing of the
            whole SD card and repopulate BOTH cached trees (the .3mf tree is derived by
            filtering the full one). The mode only selects whether the tool calls
            ``printer.get_sdcard_contents()`` or ``printer.get_sdcard_3mf_files()`` and checks
            that call's return value; there is no cost or scope difference. Case-insensitive.

    Returns:
        ``{"success": True, "mode": <'full' or '3mf'>, "printer": <name>}`` on success. Errors
        are ``{"error": str}``: ``"Printer '<name>' not connected"``,
        ``"Unknown mode '<mode>'. Must be 'full' or '3mf'."``,
        ``"Failed to retrieve SD card contents"`` (the printer library reported the listing as
        failed, which it does by returning None and clearing both cached trees, so the cache
        is empty, not merely stale), or ``"Error refreshing SD card on '<name>': <exception>"``.

    Notes:
        The refresh is synchronous: the updated data is available immediately. It triggers an
        explicit re-read of the SD card contents over FTPS and repopulates the printer library's
        cache; it changes nothing on the printer. Success means the library returned a
        listing tree, and an empty card is a success with no ``children``. A listing that
        failed (a timeout, a dropped connection, a folder that could not be listed) is the
        ``Failed to retrieve SD card contents`` error, never a success. A folder the printer
        refuses to list is treated as empty. If the call raises instead (for example the FTPS
        connection cannot be opened), the error names the exception and the previous cached
        trees are left in place.
    """
    log.debug("refresh_sdcard: called for name=%s mode=%s", name, mode)
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("refresh_sdcard: printer not connected: %s", name)
        return _no_printer(name)
    mode_lower = mode.lower()
    if mode_lower not in ("full", "3mf"):
        return {"error": f"Unknown mode '{mode}'. Must be 'full' or '3mf'."}
    try:
        if mode_lower == "3mf":
            log.debug("refresh_sdcard: calling printer.get_sdcard_3mf_files() for %s", name)
            tree = printer.get_sdcard_3mf_files()
        else:
            log.debug("refresh_sdcard: calling printer.get_sdcard_contents() for %s", name)
            tree = printer.get_sdcard_contents()
        if tree is None:
            log.debug("refresh_sdcard: → error: listing failed for %s", name)
            return {"error": "Failed to retrieve SD card contents"}
        log.debug("refresh_sdcard: %s refresh complete for %s", mode_lower, name)
        return {"success": True, "mode": mode_lower, "printer": name}
    except Exception as e:
        log.error("refresh_sdcard: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error refreshing SD card on '{name}': {e}"}
