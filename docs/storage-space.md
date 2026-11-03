# SD card space

A prepared job is uploaded only when the printer's SD card is shown to have room for the job,
the plate G-code the printer unpacks from it and a reserve. It is started only when the same card
still has room for the unpacking. Source: `storage_space.py`, called from `upload_file` and
`print_file` (`tools/files.py`) under the per-printer SD-card lock.

## Why

On 2026-11-03 a prepared 1,407,822-byte `.gcode.3mf` was uploaded, verified by hash and started
on the P1S. The printer went PREPARE -> IDLE with HMS 0500-4003: it could not unpack the
6,904,233-byte `Metadata/plate_1.gcode` (the firmware writes it to
`/cache/<job>_plate_1.gcode`, as the earlier base deck's 7,027,120-byte copy there showed). Read
on a computer (`fsck.fat -n`), the 31.3 GB card, a reused dashcam card holding 28.83 GiB of
videos, had **38 free 32 KiB clusters (1.2 MB)**. Its FAT32 free-cluster summary (FSINFO) claimed
945,077. fsck also found 17 lost clusters and two file chains longer than their files. Nothing in
the server knew the card was full.

## What the printer reports

| Channel | What it says about storage | Source |
|---|---|---|
| MQTT `push_status` | State only: `home_flag` bits 8-9, or `aux` bits 12-13 on newer firmware, or the legacy `sdcard` bool. 0 no card, 1 normal, 2 abnormal, 3 read-only | Bambu Studio `DeviceManager.cpp` (`parse_home_flag`, `aux`), `DeviceCore/DevStorage.{h,cpp}` (master `79df7ae`) |
| MQTT `device.cam` / `ipcam` | `tl_external_free_kb`, `tl_external_total_kb` (KiB): timelapse storage, added to Studio 2026-04-15 (`cf23007`) for printers with built-in timelapse storage; seen in X2D captures, absent from the H2D fw 01.03 capture here and from P1P captures | same; ha-bambulab `MOCK-X2D.json`, `MOCK-P1PNOAMS.json`; `docs/captures/` |
| FTPS (port 990) | Names and sizes (`LIST`). No free-space or capacity command is documented for this dialect (implicit TLS, PASV only, no MLSD/REST/APPE) | open-bamboo-networking `research/06.03-ftps.md` |

Bambu Studio itself checks no free space before sending a print: only the SD state, and a
"less than 10MB" timelapse warning from the KiB fields (`SelectMachine.cpp`
`check_timelapse_storage_warning`). The P1S's own MQTT fields were not captured live: verbose
logging is a monitoring change, and a second MQTT session is not allowed. The P1P capture
reports `sdcard: true` and `home_flag` bits 8-9 = 1. After deployment,
`get_printer_connection_status` shows what the P1S sends under `telemetry.storage.sd_card`.

The `tl_external_*` figures are not used for admission: they describe timelapse storage, nothing
shows they are the card a job goes to, and a figure the firmware takes from the FAT summary would
have said about 29 GiB free on the 2026-11-03 card. `get_printer_connection_status` shows one, if
sent, as `telemetry.storage.timelapse_storage_free_bytes`.

## How space is measured

The capacity comes from the card. `ops/attest_sd_card.py`, run on a computer with the card
inserted, requires a clean `fsck.fat -n -v` (no wrong free-cluster summary, lost clusters or
over-long chains). It takes the data area and cluster size from the filesystem, writes
`/bambu-mcp-card.json` with a new random `card_id`, and requires fsck to be clean again. The
server, on one FTPS session of its own printer session and only while the printer reports no
job, lists the whole card with `LIST` and reads that record. The listing must be complete: a
folder the printer refuses to list (550, which bpm's listing reads as empty), a row that is not a
file or folder (a link, a device, a row that does not parse; bpm's listing skips those) or a folder
nested deeper than 32 refuses with `storage_unknown`, because what it hides can be any size. An
empty folder is fine.

```
available = data_bytes - each listed file rounded up to whole clusters - folder clusters
upload    : job + largest plate G-code + 64 MiB reserve          (all rounded to clusters)
start     : selected plate G-code + 64 MiB reserve, on the card whose card_id the upload bound
```

A folder counts its entries as FAT stores them: 32 bytes for the short entry and 32 per 13
UTF-16 units of the name (up to 21 entries, 672 bytes, for a 255-unit name), plus `.` and `..`,
at least one cluster. `available` is an upper bound on the free space, not a guarantee.

The listing does not depend on the FAT summary that was wrong. On the 2026-11-03 card the
printer's FTPS listing (869 files, 29.1309 GiB) accounted for all but about 7 MB of the 954,778
used clusters fsck counted; the unlisted part (17 lost clusters, two over-long chains) was under
1 MiB. The 64 MiB reserve covers that kind of unlisted allocation (lost clusters, over-long chains,
folder clusters still held by deleted entries: FAT folders never shrink), folder growth and
firmware logs. A file being overwritten and an earlier unpacked copy in `/cache` are not credited.

Applied to 2026-11-03: available = 954,816 x 32 KiB (31,287,410,688 bytes) minus the listed files
(31,279,065,700 bytes before rounding) is at most 8.4 MB, against 1.4 MB + 6.9 MB + 64 MiB
required, so the upload would have been refused. The card as formatted on platt afterwards (954,837 clusters, 31,288,066,048 bytes free when empty) passes once attested:
about 29 GiB available against a requirement of about 72 MiB.

## Refusals

All are `{"error": code, "detail", ...}` before anything is written or sent:

| Code | Meaning |
|---|---|
| `storage_state_unknown` | no SD state reported on this MQTT connection (a value from before a reconnect does not count) |
| `storage_no_card`, `storage_card_abnormal`, `storage_card_read_only` | the printer's reported state |
| `storage_unattested` | no `/bambu-mcp-card.json` on the card, or not a valid record |
| `storage_unknown` | the record could not be read, or the listing is incomplete (`folder` names where: a folder the printer refused, a row that is not a file or folder) |
| `storage_insufficient` | `storage` carries `available_bytes` (the figure the decision used), `required_bytes`, `required` (job, unpacked_gcode, reserve), `data_bytes`, `listed_allocated_bytes`, `files`, `folders`, `card_id`, `sd_card` |
| `storage_record_protected` | `upload_file` to, or `rename_sdcard_file` onto, `/bambu-mcp-card.json` (any case, trailing dots, or its 8.3 alias): only the attestation script writes the record, so a caller cannot claim a size |
| `storage_card_changed` | at the start, the card's record is not the one the upload bound; the admission is dropped |

A start refused for space keeps its admission: free space on the card, then call `print_file`
again. The server never deletes, formats or repairs anything to make room, and has no override.
`get_printer_connection_status` shows `telemetry.storage`: the SD state reported on this
connection, any timelapse-storage free figure (not used) and the latest space check with its age.

## Operator: attest a card

Do this after formatting a card, and for any card that is new to the printer:

```sh
# printer off, card in a computer, NOT mounted (unmount it if the desktop mounted it)
sudo python3 ops/attest_sd_card.py /dev/mmcblk0p1
```

A card that fails fsck is reported and left alone; check or format it yourself, then attest. If the card
fails after the record was written (write, read-back or the second fsck), the script deletes the record
again and says whether that worked, so a failed attestation never leaves a record the server accepts.
The record is a file on the card; the server refuses to write it (`storage_record_protected`). Re-attest
after reformatting. Moving files on the card from a computer is fine (the server lists the card each
time); a corrupted card is not, so re-attest after an unclean removal or power loss during a write.

## Limits

* The capacity is as good as the attestation; the card's identity is the record on it. A card copied
  file-for-file onto a different card carries the wrong capacity.
* Allocation the listing cannot see beyond the reserve (heavy corruption since attestation, many deleted
  entries in a folder) is not detected.
* The card's identity is not read (no CID or volume serial is visible over FTPS); a record copied onto
  another card, or written onto the card from a computer by hand, is believed.
* Between the measurement and the printer's unpacking, another client (Bambu Studio, Handy, a timelapse)
  can fill the card. The reserve absorbs some of it; the start is not watched afterwards.
* Non-job files (`upload_file` of a `.txt`, etc.) are not space-checked.
* FAT only (`fsck.fat`); exFAT cards cannot be attested by the script.
* E2E (`harness/`) runs the printer's MQTT, FTPS and the server for real against a 1 GiB fixture card
  whose large files are listed but not stored. The attestation script was checked on FAT32 loop images
  on platt: a clean image was attested; one with a wrong FSINFO free count was refused with the image
  unchanged; one made unclean between the write and the second fsck (a wrong FSINFO free count written
  to the device) was refused and the record deleted (fsck -l lists no record afterwards).
