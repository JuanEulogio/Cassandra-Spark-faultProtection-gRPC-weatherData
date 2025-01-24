# == previous tester.py ==
from collections import OrderedDict
import json
import os
import traceback
import shutil

import multiprocessing

multiprocessing.set_start_method("fork")


def warn(msg):
    print(f"🟡 Warning: {msg}")


def error(msg):
    print(f"🔴 Error: {msg}")


def info(msg):
    print(f"🔵 Info: {msg}")


ARGS = None
DEBUG = False
VERBOSE = False
TMP_DIR = "/tmp/_cs544_tester_directory"
TEST_DIR = None
DEBUG_DIR = "_autograder_results"
PROJECT_REMOTE_URL = (
    "https://git.doit.wisc.edu/cdis/cs/courses/cs544/f24/main/-/raw/main/p6/"
)

# full list of tests
INIT = None
TESTS = OrderedDict()
CLEANUP = None


def verbose(msg):
    if VERBOSE:
        print(msg)


def run_with_timeout(func, timeout):
    def wrapper(ret):
        try:
            func()
            result = None
        except Exception as e:
            result = traceback.format_exception(e)
        ret.send(result)

    ret_send, ret_recv = multiprocessing.Pipe()
    proc = multiprocessing.Process(target=wrapper, args=(ret_send,))
    proc.start()
    proc.join(timeout)
    if proc.is_alive():
        proc.terminate()
        result = "Timeout"
    else:
        result = ret_recv.recv()
    return result


# dataclass for storing test object info
class _unit_test:
    def __init__(self, func, points, timeout, desc):
        self.func = func
        self.points = points
        self.timeout = timeout
        self.desc = desc

    def run(self, ret):
        points = 0

        try:
            result = self.func()
            if not result:
                points = self.points
                result = f"PASS ({self.points}/{self.points})"
            else:
                print(f"Test {self.func.__name__} failed:\n")
                if isinstance(result, str):
                    result = result.split("\n")
                if isinstance(result, list):
                    print("\n".join(result) + "\n")
                else:
                    print(result + "\n")
        except Exception as e:
            result = traceback.format_exception(e)
            print(f"Exception in {self.func.__name__}:\n")
            print("\n".join(result) + "\n")

        ret.send((points, result))


# init decorator
def init(init_func):
    global INIT
    INIT = init_func
    return init_func


# test decorator
def test(points, timeout=None, desc=""):
    def wrapper(test_func):
        TESTS[test_func.__name__] = _unit_test(test_func, points, timeout, desc)

    return wrapper


# debug dir decorator
def debug(debug_func):
    global DEBUG
    DEBUG = debug_func
    return debug_func


# cleanup decorator
def cleanup(cleanup_func):
    global CLEANUP
    CLEANUP = cleanup_func
    return cleanup_func


# get arguments
def get_args():
    return ARGS


# lists all tests
def list_tests():
    for test_name, test in TESTS.items():
        print(f"{test_name}({test.points}): {test.desc}")


# run all tests
def run_tests():
    results = {
        "score": 0,
        "full_score": 0,
        "tests": {},
    }

    for test_name, test in TESTS.items():
        if VERBOSE:
            print(f"===== Running Test {test_name} =====")

        results["full_score"] += test.points

        ret_send, ret_recv = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=test.run, args=(ret_send,))
        proc.start()
        proc.join(test.timeout)
        if proc.is_alive():
            proc.terminate()
            points = 0
            result = "Timeout"
        else:
            (points, result) = ret_recv.recv()

        if VERBOSE:
            print(result)
        results["score"] += points
        results["tests"][test_name] = result

    assert results["score"] <= results["full_score"]
    if VERBOSE:
        print("===== Final Score =====")
        print(json.dumps(results, indent=4))
        print("=======================")

    if DEBUG:
        debug_abs_path = f"{TEST_DIR}/{DEBUG_DIR}"
        shutil.rmtree(debug_abs_path, ignore_errors=True)
        shutil.copytree(src=TMP_DIR, dst=debug_abs_path, dirs_exist_ok=True)
        print(f"Run results are stored to {debug_abs_path}")

    return results


# save the result as json
def save_results(results):
    output_file = f"{TEST_DIR}/score.json"
    print(f"Output written to: {output_file}")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)


def check_files(test_dir, required_files):
    if not os.path.isdir(f"{test_dir}/.git"):
        warn(f"{test_dir} is not a repository")

    missing_files = []
    for file in required_files:
        if not os.path.exists(f"{test_dir}/{file}"):
            missing_files.append(file)
    if len(missing_files) > 0:
        msg = ", ".join(missing_files)
        warn(f"the following required files are missing: {msg}")


def check_for_updated_files():
    files = ["autograde.py", "Dockerfile"]
    for file in files:
        # check local existence
        if not os.path.exists(file):
            error(f"{file} is not found in the current directory.")
            continue

        # get md5sum of the remote file
        remote_md5sum, _ = subprocess.run(
            f"wget {PROJECT_REMOTE_URL}{file} -O - | md5sum",
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.split()
        # get md5sum of the local file
        local_md5sum, _ = subprocess.run(
            f"md5sum {file}", shell=True, capture_output=True, text=True
        ).stdout.split()

        if remote_md5sum != local_md5sum:
            print("=" * 40)
            warn(f"{file} is not the same as the file in the repository.")
            print(
                "This could be: (1) You may have modified the file. (2) The file has been updated."
            )
            print("=" * 40)


def tester_main(required_files=[]):
    global ARGS, VERBOSE, TEST_DIR, DEBUG

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dir", type=str, default=".", help="path to your repository"
    )
    parser.add_argument("-l", "--list", action="store_true", help="list all tests")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-g",
        "--debug",
        action="store_true",
        help="create a debug directory with the files used while testing",
    )
    parser.add_argument(
        "-k",
        "--skip-check",
        action="store_true",
        default=False,
        help="skip checking for updated files",
    )
    parser.add_argument(
        "-c",
        "--clean-docker",
        action="store_true",
        default=False,
        help="clean build docker image before running tests",
    )
    args = parser.parse_args()

    if not args.skip_check:
        check_for_updated_files()

    ARGS = args

    if args.list:
        list_tests()
        return

    VERBOSE = args.verbose
    DEBUG = args.debug
    test_dir = args.dir
    if not os.path.isdir(test_dir):
        error("invalid path")
        return
    TEST_DIR = os.path.abspath(test_dir)

    # check if required files are present
    check_files(test_dir, required_files)

    # make a copy of the code
    def ignore(_dir_name, _dir_content):
        return [".git", ".github", "__pycache__", ".gitignore", "*.pyc", DEBUG_DIR]

    shutil.copytree(src=TEST_DIR, dst=TMP_DIR, dirs_exist_ok=True, ignore=ignore)
    os.chdir(TMP_DIR)

    # run init
    if INIT:
        ret = INIT()
        if ret != None:
            result = f"Init failed: {ret}"
            error(result)
            save_results(result)
            exit(-1)

    # run tests
    results = run_tests()
    save_results(results)

    # move server.out to src
    shutil.copy(f"{output_dir_wrt_vm}/server.out", f"{TEST_DIR}/server.out")
    info("server stdout is saved to server.out")

    # cleanup code after all tests run
    shutil.rmtree(TMP_DIR, ignore_errors=True)

    # run cleanup
    if CLEANUP:
        ret = CLEANUP()
        if ret != None:
            result = f"Cleanup failed: {ret}"
            error(result)
            exit(-1)


# == p6 autograder part ==

import os
import time
import subprocess
import concurrent.futures
import json
import threading
import traceback
import argparse


def get_environment():
    environment = os.environ.copy()
    environment["DOCKER_CLI_HINTS"] = "false"
    return environment


def stop_remove_all_containers():
    container_ids = (
        subprocess.run(
            ["docker", "ps", "-aq"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )

    if container_ids:
        # Stop the containers if any are running
        stop_command = ["docker", "stop"] + container_ids.split()
        result = subprocess.run(
            stop_command,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remove the containers
        rm_command = ["docker", "rm"] + container_ids.split()
        result = subprocess.run(
            rm_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        print("[CLEANUP] Removing containers.")


def docker_image_exists(image_name):
    result = subprocess.run(
        ["docker", "images", "-q", image_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return bool(result.stdout.strip())


@cleanup
def _cleanup():
    print("Stopping all existing containers")
    stop_remove_all_containers()

    args = ARGS
    if args.clean_docker:
        # Clean up the docker images
        print("Cleaning up docker")
        cmd = "docker system prune -af"
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL)
    else:
        # remove every image except p6-base
        print("Removing all images except p6-base")
        cmd = "docker images --format '{{.Repository}}:{{.Tag}}' |  grep -Ev '^<none>:|:<none>$' | grep -v 'p6-base' | xargs -r docker rmi"
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL)

    print("Cleanup done")


def wait_for_all_three_up():
    up_loop = 0
    while up_loop < 10:
        count = 0
        print(
            "Waiting for the cassandra cluster to start up - This is going to take a while!!"
        )
        all_three_up = False
        command_to_run = "docker exec -it p6-db-1 nodetool status"

        while not all_three_up and count < 100:
            time.sleep(1)  # Wait a little bit

            # Read the result of nodetool status
            result = subprocess.run(
                command_to_run,
                capture_output=True,
                text=True,
                shell=True,
                env=os.environ.copy(),
            )

            all_three_up = result.stdout.count("UN") >= 3
            count += 1

        time.sleep(5)
        if count >= 100:
            _cleanup()
            subprocess.check_output(
                "docker compose up -d", shell=True, env=get_environment()
            )
            up_loop += 1
            time.sleep(10)
        else:
            break

    print("Cassandra cluster has started up")


def wait_for_one_dead():
    print("Waiting for a cassandra node to be down")
    one_node_dead = False
    command_to_run = "docker exec -it p6-db-1 nodetool status"

    while not one_node_dead:
        time.sleep(1)  # Wait a little bit

        # Read the result of nodetool status
        result = subprocess.run(
            command_to_run,
            capture_output=True,
            text=True,
            shell=True,
            env=os.environ.copy(),
        )

        one_node_dead = result.stdout.count("DN") >= 1

    time.sleep(5)
    print("Detected a down cassandra node")


output_dir_wrt_container = "autograder_result"
output_dir_wrt_vm = os.path.join("src", output_dir_wrt_container)


def proto_compile():
    environment = get_environment()
    # Testing if the the proto file can be profiled
    try:
        proto_build_cmd = f'docker exec -w /src p6-db-1 sh -c "python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. station.proto " '
        # Run the proto compilation command, blocking until it finishes
        subprocess.run(proto_build_cmd, shell=True, env=environment, check=True)

        # Check if the generated files exist
        generated_files = ["src/station_pb2.py", "src/station_pb2_grpc.py"]

        if all(os.path.exists(file) for file in generated_files) and all(
            os.path.getsize(file) > 0 for file in generated_files
        ):
            return None
        else:
            return "Error compiling matchdb.proto, didn't find generated files"
    except Exception as e:
        return "Error compiling matchdb.proto"


@init
def init():
    global output_dir_wrt_container

    # Determine where the autograder will write the results to
    environment = get_environment()

    subprocess.check_output(f"rm -rf {output_dir_wrt_vm}", shell=True, env=environment)
    os.makedirs(output_dir_wrt_vm, exist_ok=True)

    # Build the p6 base image
    _cleanup()

    if not docker_image_exists("p6-base"):
        print("Building the p6 base image")
        subprocess.check_output(
            "docker build . -t p6-base", shell=True, env=environment
        )
    else:
        print("p6-base image already exists. Skipping build")

    # Start up the docker container
    print("Running docker compose up")
    subprocess.check_output("docker compose up -d", shell=True, env=environment)

    # Wait for the cluster to be initialized
    wait_for_all_three_up()

    time.sleep(1)

    # build the proto file
    proto_compile()


@test(10)
def server_run():
    # Start up the server
    environment = get_environment()
    try:
        server_start_cmd = f'docker exec -d -w /src p6-db-1 sh -c "python3 -u server.py >> {output_dir_wrt_container}/server.out" '
        subprocess.run(server_start_cmd, shell=True, env=environment)
        # If the "Server started" appears in the output, then the server has started
        for i in range(180):
            if os.path.exists(f"{output_dir_wrt_vm}/server.out"):
                with open(f"{output_dir_wrt_vm}/server.out", "r") as f:
                    server_output = f.read()
                    if "Server started" in server_output:
                        return None
            time.sleep(1)
        return "Server did not start successfully after 180 seconds - \
                do you forgot to print server started in your init function in server.py"

    except Exception as e:
        return "Error running server.py"


@test(10)
def station_schema():
    environment = get_environment()
    try:
        # Run the ClientStationSchema command
        schema_cmd = (
            f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientStationSchema.py" '
        )
        client = subprocess.run(
            schema_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        client_outputs = client_outputs.lower().strip().split("\n")

        expected_terms = {
            "create table": ["weather.stations"],
            "id": ["text"],
            "date": ["date"],
            "name": ["text", "static"],
            "record": ["station_record"],
            "primary key": ["id"],
            "clustering order by": ["date"],
        }

        for search_txt, all_excepted_words in expected_terms.items():
            expect_line = None
            for line in client_outputs:
                if search_txt in line:
                    expect_line = line
                    break
            if expect_line == None:
                return f"Couldn't find expected terms {search_txt} in ClientStationSchema output"

            for expect_word in all_excepted_words:
                if expect_word not in expect_line:
                    return f"Couldn't find expected terms {expect_word} in ClientStationSchema output for {search_txt}"

    except Exception as e:
        return "Error running ClientStationSchema.py"


@test(20)
def station_name():
    environment = get_environment()
    try:
        # Run the ClientStationName command
        name_cmd = f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientStationName.py US1WIMR0003" '
        client = subprocess.run(
            name_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        client_outputs = client_outputs.lower().strip()
        expected_txt = "amberg 1.3 sw"
        if expected_txt not in client_outputs:
            return f"couldn't find txt {expected_txt} in output {client_outputs}"

    except Exception as e:
        return "Error running ClientStationName.py, expect 'amberg 1.3 sw'"


@test(20)
def record_temps():
    environment = get_environment()
    try:
        # Run the ClientRecordTemps command
        record_cmd = (
            f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientRecordTemps.py" '
        )
        client = subprocess.run(
            record_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        client_outputs = client_outputs.lower().strip()

        if "error inserting" in client_outputs:
            return "Get Error when Inserting some temp records"
    except Exception as e:
        return "Error running ClientRecordTemps.py"


@test(10)
def station_max():
    environment = get_environment()
    try:
        # Run the ClientStationMax command
        max_cmd = f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientStationMax.py USW00014837" '
        client = subprocess.run(
            max_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        last_line = client_outputs.strip().split("\n")[-1]
        # check if last_line can be converted to int
        try:
            if int(last_line) != 356:
                return f"counldn't find 356 in the output of: python3 -u ClientStationMax.py USW00014837"
        except:
            return f"couldn't parse output {last_line} as integer"
    except Exception as e:
        return "Error running ClientStationMax.py, expect 356"


@test(10)
def record_temps_after_disaster():
    # Kill one of the nodes
    environment = get_environment()
    print("Blocking testing execution to kill p6-db-2")
    subprocess.run("docker kill p6-db-2", shell=True, env=environment)

    # Waiting for node to be killed
    wait_for_one_dead()

    try:
        # Run the ClientRecordTemps command
        record_cmd = (
            f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientRecordTemps.py" '
        )
        client = subprocess.run(
            record_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        client_outputs = client_outputs.lower().strip()

        # Inserts should happen with ConsistencyLevel.ONE, so this ought to work, meaning the empty string is the expected result for error
        if "error inserting" in client_outputs:
            return "Get Error when Inserting some temp records"
    except Exception as e:
        return "Error running ClientRecordTemps.py"


@test(10)
def station_max_after_disaster():
    environment = get_environment()
    try:
        # Run the ClientRecordTemps command
        record_cmd = f'docker exec -w /src p6-db-1 sh -c "python3 -u ClientStationMax.py USW00014837" '
        client = subprocess.run(
            record_cmd,
            shell=True,
            env=environment,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Parse the output
        client_outputs = client.stdout.decode("utf-8")
        client_outputs = client_outputs.lower().strip()
        print(client_outputs)
        expected_text = "unavailable"
        if expected_text not in client_outputs:
            return f'Expect Error: "{expected_text}", but get output: {client_outputs} '

    except Exception as e:
        return "Error running ClientRecordTemps.py"


if __name__ == "__main__":
    tester_main(
        required_files=[
            "src/server.py",
            "src/ClientRecordTemps.py",
            "src/ClientStationMax.py",
            "src/ClientStationName.py",
            "src/ClientStationSchema.py",
            "src/station.proto",
            "src/ghcnd-stations.txt",
            "Dockerfile",
            "autograde.py",
            "cassandra.sh",
            "docker-compose.yml",
        ],
    )
