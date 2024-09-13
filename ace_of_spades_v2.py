#!/usr/bin/python3

import argparse
import aria2p
import copy
import json
import logging
import os
import random
import re
import requests
import retry
import subprocess
import sys
import tenacity
import time
import urllib3
import datetime
import shutil

from typing import Any, Dict, Optional
from datetime import datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--miner-id",
        help="Storage Provider miner ID (ie. f0123456)",
        type=str,
        default=os.environ.get("MINER_ID"),
        required=not os.environ.get("MINER_ID"),
    )
    parser.add_argument(
        "--fil-spid-file-path",
        help="Full file path of the `fil-spid.bash` authorization script provided by Spade.",
        type=str,
        default=os.environ.get("FIL_SPID_FILE_PATH"),
        required=not os.environ.get("FIL_SPID_FILE_PATH"),
    )
    parser.add_argument(
        "--aria2c-url",
        help="URL of the aria2c process running in daemon mode (eg. 'http://localhost:6800'). Launch the daemon with `aria2c --enable-rpc`.",
        nargs="?",
        const="http://localhost:6800",
        type=str,
        default=os.environ.get("ARIA2C_URL", "http://localhost:6800"),
        required=False,
    )
    parser.add_argument(
        "--aria2c-connections-per-server",
        help="Configures the '-x' flag in aria2c. (eg. aria2c -x8 <uri>). Default: 10",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_CONNECTIONS_PER_SERVER", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-max-concurrent-downloads",
        help="Configures the '-j' flag in aria2c. (eg. aria2c -j10). Default: 10",
        nargs="?",
        const=10,
        type=str,
        default=os.environ.get("ARIA2C_MAX_CONCURRENT_DOWNLOADS", 10),
        required=False,
    )
    parser.add_argument(
        "--aria2c-download-path",
        help="The directory into which aria2c should be configured to download files before being imported to Boost. Default: /mnt/data",
        nargs="?",
        const="/mnt/data",
        type=str,
        default=os.environ.get("ARIA2C_DOWNLOAD_PATH", "/mnt/data"),
        required=False,
    )
    parser.add_argument(
        "--disk-space-threshold",
        help="Minimum free disk space (in GB) required to continue accepting new deals. Default: 5 GB",
        nargs="?",
        const=100,  # Default to 5 GB
        type=int,
        default=os.environ.get("DISK_SPACE_THRESHOLD", 100),
        required=False,
    )
    parser.add_argument(
        "--boost-api-info",
        help="The Boost api string normally set as the BOOST_API_INFO environment variable (eg. 'eyJhbG...aCG:/ip4/192.168.10.10/tcp/3051/http')",
        type=str,
        default=os.environ.get("BOOST_API_INFO"),
        required=not os.environ.get("BOOST_API_INFO"),
    )
    parser.add_argument(
        "--boost-graphql-port",
        help="The port number where Boost's graphql is hosted (eg. 8080)",
        nargs="?",
        const=8080,
        type=int,
        default=os.environ.get("BOOST_GRAPHQL_PORT", 8080),
        required=not os.environ.get("BOOST_GRAPHQL_PORT"),
    )
    parser.add_argument(
        "--boost-delete-after-import",
        help="Whether or not to instruct Boost to delete the downloaded data after it is imported. Equivalent of 'boostd --delete-after-import'. Default: True",
        nargs="?",
        const=True,
        type=bool,
        default=os.environ.get("BOOST_DELETE_AFTER_IMPORT", True),
        required=False,
    )
    parser.add_argument(
        "--spade-deal-timeout",
        help="The time to wait between a deal appearing in Boost and appearing in Spade before considering the deal failed (or not a Spade deal) and ignoring it. Stated in seconds, with no units. Default: 3600",
        nargs="?",
        const=True,
        type=int,
        default=os.environ.get("SPADE_DEAL_TIMEOUT", 3600),
        required=False,
    )
    parser.add_argument(
        "--maximum-boost-deals-in-flight",
        help="The maximum number of deals in 'Awaiting Offline Data Import' state in Boost UI. Default: 10",
        nargs="?",
        const=10,
        type=int,
        default=os.environ.get("MAXIMUM_BOOST_DEALS_IN_FLIGHT", 10),
        required=False,
    )
    parser.add_argument(
        "--maximum-deal-requests-per-hour",
        help="The maximum number of deals requested per hour. Default 8",
        nargs="?",
        const=8,
        type=int,
        default=os.environ.get("MAXIMUM_DEAL_REQUESTS_PER_HOUR", 8),
        required=False,
    )
    parser.add_argument(
        "--complete-existing-deals-only",
        help="Setting this flag will prevent new deals from being requested but allow existing deals to complete. Useful for cleaning out the deals pipeline to debug, or otherwise. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("COMPLETE_EXISTING_DEALS_ONLY", False),
        required=False,
    )
    parser.add_argument(
        "--verbose",
        help="If enabled, logging will be greatly increased. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("VERBOSE", False),
        required=False,
    )
    parser.add_argument(
        "--debug",
        help="If enabled, logging will be thorough, enabling debugging of deep issues. Default: False",
        nargs="?",
        const=False,
        type=bool,
        default=os.environ.get("DEBUG", False),
        required=False,
    )
    return parser.parse_args()


def get_logger(name: str, *, options: dict) -> logging.Logger:
    logger = logging.getLogger(name)
    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(f"[%(asctime)s] [%(levelname)s] {name}: %(message)s")
    stdout_handler.setFormatter(formatter)
    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)
    return logger


options = parse_args()
json_rpc_id = 1
aria2 = None
log = get_logger("Ace", options=options)
log_retry = get_logger("RETRYING", options=options)
log_aria2 = get_logger("Aria2", options=options)
log_boost = get_logger("Boost", options=options)
log_request = get_logger("Request", options=options)
log_filds = get_logger("fil-ds", options=options)

ELIGIBLE_PIECES_ENDPOINT = "https://api.spade.storage/sp/eligible_pieces"
INVOKE_ENDPOINT = "https://api.spade.storage/sp/invoke"
PENDING_PROPOSALS_ENDPOINT = "https://api.spade.storage/sp/pending_proposals"


def check_free_disk_space(path: str) -> float:
    """Check the free disk space in GB."""
    total, used, free = shutil.disk_usage(path)
    return free / (1024 ** 3)  # Convert to GB


def eligible_pieces(*, options: dict) -> dict:
    log.debug("Querying for eligible pieces")
    response = request_handler(
        url=ELIGIBLE_PIECES_ENDPOINT,
        method="get",
        parameters={"timeout": 15, "allow_redirects": True},
        log_name="eligible_pieces",
        miner_auth_header={"add": True},
    )
    if response == None:
        return None
    return response


def invoke_deal(*, piece_cid: str, tenant_policy_cid: str, options: dict) -> dict:
    log.debug("Invoking a new deal")
    response = request_handler(
        url=INVOKE_ENDPOINT,
        method="post",
        parameters={"timeout": 15, "allow_redirects": True},
        log_name="invoke_deal",
        miner_auth_header={
            "add": True,
            "stdin": f"call=reserve_piece&piece_cid={piece_cid}&tenant_policy={tenant_policy_cid}",
        },
    )

    if response == None:
        return None

    if "info_lines" not in response:
        log.error(f"Invoke_deals not returning info_lines. Check API. Error code: {response['response_code']}")
        return None
    response["piece_cid"] = re.findall(r"baga[a-zA-Z0-9]+", response["info_lines"][0])[0]
    log.debug(f"New deal requested: {response}")
    return response


def pending_proposals(*, options: dict) -> list:
    log.debug("Querying pending proposals")
    response = request_handler(
        url=PENDING_PROPOSALS_ENDPOINT,
        method="get",
        parameters={"timeout": 10, "allow_redirects": True},
        log_name="pending_proposals",
        miner_auth_header={"add": True},
    )
    if response == None:
        return None
    if "response" not in response:
        log.error(f"No reponse from pending_proposal. Check API. Error code: {response['response_code']}")
#        log.error(response['response_code'])
        return None

    return response["response"]


def boost_import(*, options: dict, deal_uuid: str, file_path: str) -> bool:
    global json_rpc_id

    log.info(f"Importing deal to boost: UUID: {deal_uuid}, Path: {file_path}")
    boost_bearer_token = options.boost_api_info.split(":")[0]
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    boost_port = options.boost_api_info.split(":")[1].split("/")[4]
    headers = {"Authorization": f"Bearer {boost_bearer_token}", "content-type": "application/json"}
    payload = {
        "method": "Filecoin.BoostOfflineDealWithData",
        "params": [
            deal_uuid,
            file_path,
            options.boost_delete_after_import,
        ],
        "jsonrpc": "2.0",
        "id": json_rpc_id,
    }

    response = request_handler(
        url=f"http://{boost_url}:{boost_port}/rpc/v0",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload), "headers": headers},
        log_name="boost_import",
        miner_auth_header={"add": False},
    )
    json_rpc_id += 1
    if response == None:
        return None
    elif "error" in response:
        log.warning(f"Import to boost failed with error: {response['error']}")
        return None
    elif "result" in response and response["result"]["Accepted"]:
        log.info(f"Deal imported to boost: {deal_uuid}")
        return True
    else:
        log.error(f"Deal failed to be imported to boost: {response}")
        return None


def get_boost_deals(*, options: dict) -> Any:
    # ToDo: Filter out deals not managed by Spade by comparing against list of pending_proposals or filtering by the client address. Currently all deals are expected to be Spade deals.
    log.debug("Querying deals from boost")
    boost_url = options.boost_api_info.split(":")[1].split("/")[2]
    payload = {
        "query": "query {deals(limit: 1000, filter: {IsOffline: true, Checkpoint: Accepted}) {deals {ID CreatedAt Checkpoint IsOffline Err PieceCid Message}totalCount}}"
    }

    response = request_handler(
        url=f"http://{boost_url}:{options.boost_graphql_port}/graphql/query",
        method="post",
        parameters={"timeout": 30, "data": json.dumps(payload)},
        log_name="get_boost_deals",
        miner_auth_header={"add": False},
    )
    if response == None:
        return None
    else:
        deals = response["data"]["deals"]["deals"]
        return [d for d in deals if d["Message"] == "Awaiting Offline Data Import"]


def request_handler(*, url: str, method: str, parameters: dict, log_name: str, miner_auth_header: dict) -> bool:
    try:
        return make_request(
            url=url, method=method, parameters=parameters, log_name=log_name, miner_auth_header=miner_auth_header
        )
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.INFO),
)
def make_request(*, url: str, method: str, parameters: dict, log_name: str, miner_auth_header: dict) -> Any:
    try:
        if miner_auth_header["add"]:
            if "stdin" in miner_auth_header:
                auth_token = shell(
                    command=["bash", options.fil_spid_file_path, options.miner_id], stdin=miner_auth_header["stdin"]
                )
            else:
                auth_token = shell(command=["bash", options.fil_spid_file_path, options.miner_id])

            if "headers" not in parameters:
                parameters["headers"] = {"Authorization": auth_token}
            else:
                parameters["headers"]["Authorization"] = auth_token

        if method == "post":
            response = requests.post(url, **parameters)
        if method == "get":
            response = requests.get(url, **parameters)

        res = response.json()

        # Disable logging of noisy responses
        if url != ELIGIBLE_PIECES_ENDPOINT:
            log_request.debug(f"{log_name}, Response: {res}")
    except requests.exceptions.HTTPError as e:
        log_request.error(f"{log_name}, HTTPError: {e}")
        raise Exception(f"HTTPError: {e}")
    except requests.exceptions.ConnectionError as e:
        log_request.error(f"{log_name}, ConnectionError: {e}")
        raise Exception(f"ConnectionError: {e}")
    except (TimeoutError, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout) as e:
        log_request.error(f"{log_name}, Timeout: {e}")
        raise Exception(f"Timeout: {e}")
    except:
        log_request.error(f"{log_name}, Timeout: {e}")
        raise Exception(f"Timeout: {e}")

    if response.status_code == 401:
        if "error_lines" in res and "in the future" in "".join(res["error_lines"]):
            log_request.info(
                f'Known issue in Spade: the auth token generated by fil-spid.bash is "in the future" according to Spade. Retrying.'
            )
            raise Exception("Auth token is in the future.")
        else:
            log_request.error(f"{log_name}, received 401 Unauthorized: {res}")
            return None
    if response.status_code == 403:
        log_request.error(f"{log_name}, received 403 Forbidden: {res}")
        return None

    return res


def shell(*, command: list, stdin: Optional[str] = None) -> str:
    # This is gross and unfortunately necessary. fil-spid.bash is too complex to reimplement in this script.
    try:
        process = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, universal_newlines=True, input=stdin
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Bash command failed with exit code {e.returncode}",
            f"Error message: {e.stderr}",
        ) from e
    return process.stdout.rstrip()


def download(*, options: dict, source: str) -> bool:
    try:
        return download_files(options=options, source=source)
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def download_files(*, options: dict, source: str) -> bool:
    if not source.startswith("http"):
        log_aria2.error(f"Error. Unknown file source: {source}")
        # ToDo handle this
    try:
        down = aria2.add_uris(
            [source],
            options={
                "max_connection_per_server": options.aria2c_connections_per_server,
                "auto_file_renaming": False,
                "dir": options.aria2c_download_path,
                "check-certificate": False
            },
        )
    except:
        log_aria2.error("Aria2c failed to start download of file.")
        raise Exception("Aria2c failed to start download of file.")

    # This is a hack. For some reason 'max_connection_per_server' is ignored by aria2.add_uris().
    # Since this is incredibly important for this use case, this workaround is required.
    down.options.max_connection_per_server = options.aria2c_connections_per_server
    log_aria2.info(f"Downloading file: {source}")
    return True


def get_downloads() -> dict:
    try:
        return get_aria_downloads()
    except tenacity.RetryError as e:
        log.error("Retries failed. Moving on.")
        return None


@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=6, multiplier=2),
    after=tenacity.after.after_log(log_retry, logging.WARN),
)
def get_aria_downloads() -> dict:
    return aria2.get_downloads()


def download_error(e, i):
    try:
        downloads = e.get_downloads()
        for down in downloads:
            if down.gid == i:
                log.info(f"Retrying download due to aria2 error: {down.error_message}")
                response = e.retry_downloads([down], False)
                log.debug(f"Retry status: {response}")
    except:
        log.error(f"Failed to retry download of {i}")


def is_download_in_progress(*, url: str, downloads: list) -> bool:
    for d in downloads:
        for f in d.files:
            for u in f.uris:
                if u["uri"] == url:
                    return True
    return False


def request_deal(*, options: dict) -> Any:
    log.info("Requesting a new deal from Spade")
    # Select a deal
    pieces = eligible_pieces(options=options)
    if pieces == None:
        return None

    if "response" not in pieces:
        log.error(f"No deal pieces returned or 'response' key not found. Check API.")
        return None

    if len(pieces["response"]) < 1:
        log.error(f"Error. No deal pieces returned. Check API.")
        return None
    # Randomly select a deal from those returned
    deal_number = random.randint(0, len(pieces["response"]) - 1)
    deal = pieces["response"][deal_number]
    log.debug(f"Deal selected: {deal}")

    # Create a reservation
    return invoke_deal(piece_cid=deal["piece_cid"], tenant_policy_cid=deal["tenant_policy_cid"], options=options)


def setup_aria2p(*, options: dict) -> Any:
    global aria2

    try:
        aria2 = aria2p.API(
            aria2p.Client(
                host=":".join(options.aria2c_url.split(":")[:-1]), port=options.aria2c_url.split(":")[-1], secret=""
            )
        )
        aria2.get_global_options().max_concurrent_downloads = options.aria2c_max_concurrent_downloads
    except:
        log_aria2.error(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")
        raise Exception(f"Could not connect to an aria2 daemon running at '{options.aria2c_url}'")

    try:
        aria2.listen_to_notifications(
            threaded=True,
            on_download_start=None,
            on_download_pause=None,
            on_download_stop=None,
            on_download_complete=None,
            on_download_error=download_error,
            on_bt_download_complete=None,
            timeout=5,
            handle_signals=True,
        )
    except:
        log_aria2.error(f"Could not start listening to notifications from aria2c API.")
        raise Exception(f"Could not start listening to notifications from aria2c API.")


def populate_startup_state(*, options: dict) -> dict:
    state = {}
    # Read in deals from Boost. This is the only source necessary on startup as
    # the main control loop will detect and update deal status dynamically.
    deals = get_boost_deals(options=options)
    if deals == None:
        os._exit(1)
    for d in deals:
        state[d["PieceCid"]] = {
            "deal_uuid": d["ID"],
            "files": {},
            "timestamp_in_boost": time.time(),
            "status": "available_in_boost",
        }
    log.info(f"Found {len(deals)} deals in Boost")
    return state


def startup_checks(*, options: dict) -> None:
    # Ensure the file-spid.bash script exists
    if not os.path.exists(options.fil_spid_file_path):
        log.error(f"Authorization script does not exist: {options.fil_spid_file_path}")
        os._exit(1)

    # Ensure the download directory exists
    if not os.path.exists(options.aria2c_download_path):
        log.error(f"Aria2c download directory does not exist: {options.aria2c_download_path}")
        os._exit(1)

@tenacity.retry(
    wait=tenacity.wait_exponential(min=1, max=10),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(subprocess.CalledProcessError),
    after=tenacity.after_log(logging.getLogger("RetryLogger"), logging.WARNING)
)
def segment_based_download(piece_cid: str, assembly_cmd: str, options: dict) -> Optional[str]:
    try:
        log_filds.info(f"Starting segmentation-based download for piece CID: {piece_cid}")
        
        # Extract the command to get the authorization token
        auth_cmd_match = re.search(r"\$\((.+?)\)", assembly_cmd)
        if auth_cmd_match:
            auth_cmd = auth_cmd_match.group(1)
            
            # Run the command to get the authorization token
            auth_token_result = subprocess.run(
                auth_cmd,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=options.aria2c_download_path  # Set working directory
            )
            auth_token = auth_token_result.stdout.decode().strip()
            log_filds.info(f"Authorization token obtained: {auth_token}")
            
            # Remove "echo" and "sh" from the command and construct the new command
            clean_cmd = assembly_cmd.replace("echo ", "").replace(" | sh", "")
            clean_cmd = re.sub(r"\$\(.+?\)", auth_token, clean_cmd)

            # Split the command into parts
            cmd_parts = clean_cmd.split('|')
            process = None

            for i, cmd in enumerate(cmd_parts):
                cmd = cmd.strip()
                if i == 0:
                    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=options.aria2c_download_path)
                else:
                    process = subprocess.Popen(cmd, shell=True, stdin=process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=options.aria2c_download_path)

            if process:
                stdout, stderr = process.communicate()
                if process.returncode != 0:
                    log_filds.error(f"Segmentation-based download failed for piece CID: {piece_cid}, Error: {stderr.decode()}")
                    raise subprocess.CalledProcessError(process.returncode, cmd_parts, output=stdout, stderr=stderr)
                    return None
                else:
                    log_filds.info(f"Segmentation-based download complete for piece CID: {piece_cid}")
                    log_filds.info(f"Command output: {stdout.decode()}")
                    # Replace the first 12 characters of piece_cid with "bafkzcibcaap"
                    modified_piece_cid = "bafkzcibcaap" + piece_cid[12:]
                    # Return the file path with piece_cid and ".frc58" extension
                    file_path = os.path.join(options.aria2c_download_path, f"{modified_piece_cid}.frc58")
                    return file_path
                    #return f"{piece_cid}.frc58"
    except subprocess.CalledProcessError as e:
        log_filds.error(f"Segmentation-based download failed for piece CID: {piece_cid}, Error: {e.stderr.decode()}")
        return None


def main() -> None:
    global options
    startup_checks(options=options)
    log.info("Connecting to Aria2c...")
    setup_aria2p(options=options)
    log.info("Starting Ace of Spades...")

    deals_in_error_state = []
    state = populate_startup_state(options=options)
    # Example: state = {
    #     '<piece_cid>': {
    #         'deal_uuid': '<deal_uuid>',
    #         'files': {
    #             'http://foo': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #             'http://foo2': 'incomplete',  # can be one of ['incomplete', '<file_path>']
    #         },
    #         'timestamp_in_boost': '1234567890',  # The timestamp at which Ace detected the deal in Boost. Used for weeding out an edge case of stale deals.
    #         'status': 'invoked'  #can be one of ['invoked','available_in_boost','downloading','downloaded']
    #     }
    # }

    # Control loop to take actions, verify outcomes, and otherwise manage Spade deals

#    max_deals_per_hour = 9
    current_deals_this_hour = 0
    previous_hour = 0

    while True:
        if options.verbose:
            log.info(f"Loop start state: {json.dumps(state, indent=4)}")
        else:
            log.debug(f"Loop start state: {json.dumps(state, indent=4)}")

        # Request deals from Spade
        if not options.complete_existing_deals_only:
            if len(state) < options.maximum_boost_deals_in_flight:
                for i in range(len(state), options.maximum_boost_deals_in_flight):
                    current_date = datetime.now()
                    current_hour = current_date.hour
                    if current_hour != previous_hour:
                        previous_hour = current_hour
                        current_deals_this_hour = 0
                    free_space = check_free_disk_space(options.aria2c_download_path)
                    log.info(f"Free disk space: {free_space:.2f} GB")
                    if free_space > options.disk_space_threshold:
                        if current_deals_this_hour < options.maximum_deal_requests_per_hour:
                            log.info(f'max deals per hour not exceeded: {current_deals_this_hour}')
                            log.info(f'sleeping for 120 seconds')
#                            time.sleep(120)
                            new_deal = request_deal(options=options)
                            if new_deal != None:
                                if new_deal["piece_cid"] not in deals_in_error_state:
                                    log.debug(f'adding {new_deal["piece_cid"]} to state')
                                    state[new_deal["piece_cid"]] = {
                                        "deal_uuid": "unknown",
                                        "files": {},
                                        "timestamp_in_boost": time.time(),
                                        "status": "invoked",
                                    }
                                    log.info(f'New deal found in Boost: {new_deal["piece_cid"]}')
                                    current_deals_this_hour += 1
                        else:
                            log.info(f'number of deals per hour exceeded')
                    else:
                        log.info(f'free disk space threshold breached')

        log.debug(f"request state: {json.dumps(state, indent=4)}")

        # Identify when deals are submitted to Boost
        deals = get_boost_deals(options=options)
        if deals != None:
            for d in deals:
                if d["PieceCid"] not in deals_in_error_state:
                    if d["PieceCid"] not in state:
                        # Fallback necessary during certain Ace restart scenarios
                        state[d["PieceCid"]] = {
                            "deal_uuid": d["ID"],
                            "files": {},
                            "timestamp_in_boost": time.time(),
                            "status": "available_in_boost",
                        }
                    if state[d["PieceCid"]]["status"] == "invoked":
                        state[d["PieceCid"]]["status"] = "available_in_boost"
                        state[d["PieceCid"]]["timestamp_in_boost"] = time.time()

        log.debug(f"identify state: {json.dumps(state, indent=4)}")

        # Edge case. If a deal shows up in Boost, but does not appear in Spade for more than 1 hour (3600 seconds),
        # consider the deal to be failed and remove it from consideration. Without this check, this failure scenario can
        # prevent Ace from maintaining its maximum_boost_deals_in_flight.
        for s in list(state):
            if state[s]["status"] == "available_in_boost":
                if state[s]["timestamp_in_boost"] < (time.time() - options.spade_deal_timeout):
                    log.warning(
                        f"Deal can be seen in Boost, but has not appeared in Spade for more than {options.spade_deal_timeout} seconds. Considering the deal to be either failed or not a Spade deal: {s}"
                    )
                    del state[s]
                    deals_in_error_state.append(s)

        proposals = pending_proposals(options=options)
        downloads = get_downloads()

        # Handle Spade errors in creating deals
        log.debug(f"deals_in_error_state: {deals_in_error_state}")
        if proposals != None:
            if "recent_failures" in proposals:
                for p in proposals["recent_failures"]:
                    if p["piece_cid"] in state:
                        log.warning(
                            f'Spade encountered an error with {p["piece_cid"]}: `{p["error"]}`. Ignoring deal and moving on.'
                        )
                        del state[p["piece_cid"]]
                        deals_in_error_state.append(p["piece_cid"])

        # Start downloading deal files
        if proposals is not None and downloads is not None:
            for p in proposals["pending_proposals"]:
                if p["piece_cid"] in state:
                    if state[p["piece_cid"]]["status"] == "available_in_boost":
                        state[p["piece_cid"]]["deal_uuid"] = p["deal_proposal_id"]

                        # Start download of all files in deal
                        running = 0
                        if p["segmentation_type"] == "frc58":
                            log.info(f"Segmentation-based download for piece CID: {p['piece_cid']}")
                            assembly_cmd = p["sample_assembly_cmd"].replace("{{downloaded_or_assembled_file}}", options.aria2c_download_path)
                            try:
                                assembled_file_path = segment_based_download(piece_cid=p["piece_cid"], assembly_cmd=assembly_cmd, options=options)
                                if assembled_file_path:
                                    state[p["piece_cid"]]["files"][p["piece_cid"]] = assembled_file_path
                                    state[p["piece_cid"]]["status"] = "downloaded"
                                    log.info(f"Segmentation-based download complete for piece CID: {p['piece_cid']}")
                                else:
                                    log.error(f"Segmentation-based download failed for piece CID: {p['piece_cid']}")
                            except subprocess.CalledProcessError:
                                log.error(f"Failed to download for piece CID: {p['piece_cid']} after retries.")
                        else:
                            for source in p["data_sources"]:
                                # Notice and ingest preexisting downloads, whether by a previous Ace process or manual human intervention
                                if is_download_in_progress(url=source, downloads=downloads):
                                    state[p["piece_cid"]]["files"][source] = "incomplete"
                                    running += 1
                                else:
                                    if download(source=source, options=options):
                                        running += 1
                                        state[p["piece_cid"]]["files"][source] = "incomplete"
                                    else:
                                        log.error(f"Failed to start download of URL: {source}")
                            if running == len(p["data_sources"]):
                                state[p["piece_cid"]]["status"] = "downloading"

        log.debug(f"download state: {json.dumps(state, indent=4)}")

        # Check for completed downloads
        downloads = get_downloads()
        if downloads is not None:
            for down in downloads:
                if down.is_complete:
                    for s in state.keys():
                        # Ensure files have been populated before proceeding
                        if bool(state[s]["files"]):
                            for source in state[s]["files"].keys():
                                # If the completed downloads' source matches this deal, change 'incomplete' to the file path
                                if source == down.files[0].uris[0]["uri"]:
                                    state[s]["files"][source] = str(down.files[0].path)
                                    break
                            if all([v != "incomplete" for v in state[s]["files"].values()]):
                                state[s]["status"] = "downloaded"
                                log.info(f"Download complete: {s}")
                    # Cleanup download from Aria2c
                    down.purge()

        # Handle segmentation-based downloads in the state
        for s in list(state):
            if state[s]["status"] == "downloaded" and 'frc58' in state[s].get("segmentation_type", ""):
                log.info(f"Segmentation-based deal complete: {s}")
                del state[s]


        log.debug(f"completed download state: {json.dumps(state, indent=4)}")

        # Import deals to Boost
        for s in list(state):
            outcome = []
            if state[s]["status"] == "downloaded":
                for f in state[s]["files"].keys():
                    out = boost_import(options=options, deal_uuid=state[s]["deal_uuid"], file_path=state[s]["files"][f])
                    if out != None:
                        outcome.append(out)

                # If all files for this deal have been imported, delete the deal from local state
                if all(outcome):
                    log.info(f"Deal complete: {s}")
                    del state[s]

        if options.verbose:
            log.info(f"Loop end state: {json.dumps(state, indent=4)}")
        else:
            log.debug(f"Loop end state: {json.dumps(state, indent=4)}")

        if options.complete_existing_deals_only:
            if len(state) == 0:
                log.info("No more deals in flight. Exiting due to --complete-existing-deals-only flag.")
                os._exit(0)

        log.info("End of loop. Sleeping for 15 seconds")
        time.sleep(15)


if __name__ == "__main__":
    main()
