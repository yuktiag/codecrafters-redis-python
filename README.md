import socket
import threading
import time

store = {}


def compare_ids(id1, id2):

    ms1, seq1 = map(int, id1.split("-"))
    ms2, seq2 = map(int, id2.split("-"))

    if ms1 > ms2:
        return 1

    if ms1 < ms2:
        return -1

    if seq1 > seq2:
        return 1

    if seq1 < seq2:
        return -1

    return 0


def handle_client(client):

    in_multi = False
    transaction_queue = []

    while True:

        try:

            data = client.recv(4096)

            if not data:
                break

            data = data.decode()

            parts = data.split("\r\n")

            command_parts = []

            for part in parts:

                if (
                    part
                    and not part.startswith("*")
                    and not part.startswith("$")
                ):
                    command_parts.append(part)

            if len(command_parts) == 0:
                continue

            cmd = command_parts[0].upper()

            # =========================
            # MULTI
            # =========================
            if cmd == "MULTI":

                in_multi = True
                transaction_queue = []

                client.send(b"+OK\r\n")

            # =========================
            # EXEC
            # =========================
            elif cmd == "EXEC":

                if not in_multi:

                    client.send(
                        b"-ERR EXEC without MULTI\r\n"
                    )

                    continue

                responses = []

                for queued_command in transaction_queue:

                    qcmd = queued_command[0].upper()

                    # -----------------
                    # SET
                    # -----------------
                    if qcmd == "SET":

                        key = queued_command[1]
                        value = queued_command[2]

                        store[key] = value

                        responses.append(
                            b"+OK\r\n"
                        )

                    # -----------------
                    # GET
                    # -----------------
                    elif qcmd == "GET":

                        key = queued_command[1]

                        if key in store:

                            value = store[key]

                            responses.append(
                                f"${len(value)}\r\n{value}\r\n".encode()
                            )

                        else:

                            responses.append(
                                b"$-1\r\n"
                            )

                    # -----------------
                    # INCR
                    # -----------------
                    elif qcmd == "INCR":

                        key = queued_command[1]

                        if key not in store:
                            store[key] = "0"

                        value = store[key]

                        try:
                            number = int(value)

                        except:

                            responses.append(
                                b"-ERR value is not an integer or out of range\r\n"
                            )

                            continue

                        number += 1

                        store[key] = str(number)

                        responses.append(
                            f":{number}\r\n".encode()
                        )

                result = (
                    f"*{len(responses)}\r\n"
                ).encode()

                for r in responses:
                    result += r

                client.send(result)

                in_multi = False
                transaction_queue = []

            # =========================
            # QUEUE COMMANDS
            # =========================
            elif in_multi:

                transaction_queue.append(
                    command_parts
                )

                client.send(
                    b"+QUEUED\r\n"
                )

            # =========================
            # PING
            # =========================
            elif cmd == "PING":

                client.send(
                    b"+PONG\r\n"
                )

            # =========================
            # ECHO
            # =========================
            elif cmd == "ECHO":

                message = command_parts[1]

                client.send(
                    f"${len(message)}\r\n{message}\r\n".encode()
                )

            # =========================
            # SET
            # =========================
            elif cmd == "SET":

                key = command_parts[1]
                value = command_parts[2]

                store[key] = value

                client.send(
                    b"+OK\r\n"
                )

            # =========================
            # GET
            # =========================
            elif cmd == "GET":

                key = command_parts[1]

                if key in store:

                    value = store[key]

                    client.send(
                        f"${len(value)}\r\n{value}\r\n".encode()
                    )

                else:

                    client.send(
                        b"$-1\r\n"
                    )

            # =========================
            # INCR
            # =========================
            elif cmd == "INCR":

                key = command_parts[1]

                if key not in store:
                    store[key] = "0"

                value = store[key]

                try:
                    number = int(value)

                except:

                    client.send(
                        b"-ERR value is not an integer or out of range\r\n"
                    )

                    continue

                number += 1

                store[key] = str(number)

                client.send(
                    f":{number}\r\n".encode()
                )

            # =========================
            # XADD
            # =========================
            elif cmd == "XADD":

                stream_key = command_parts[1]
                entry_id = command_parts[2]

                field = command_parts[3]
                value = command_parts[4]

                if stream_key not in store:
                    store[stream_key] = []

                # -------------------------
                # AUTO GENERATED IDS
                # -------------------------
                if entry_id.endswith("*"):

                    ms = entry_id.split("-")[0]

                    seq = 0

                    for item in store[stream_key]:

                        old_id = item["id"]

                        old_ms, old_seq = old_id.split("-")

                        if old_ms == ms:

                            seq = max(
                                seq,
                                int(old_seq) + 1
                            )

                    entry_id = f"{ms}-{seq}"

                # -------------------------
                # INVALID ID
                # -------------------------
                if entry_id == "0-0":

                    client.send(
                        b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
                    )

                    continue

                # -------------------------
                # CHECK STREAM ORDER
                # -------------------------
                if len(store[stream_key]) > 0:

                    last_id = store[stream_key][-1]["id"]

                    if compare_ids(entry_id, last_id) <= 0:

                        client.send(
                            b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
                        )

                        continue

                # -------------------------
                # STORE STREAM ENTRY
                # -------------------------
                store[stream_key].append({
                    "id": entry_id,
                    "fields": [field, value]
                })

                client.send(
                    f"${len(entry_id)}\r\n{entry_id}\r\n".encode()
                )

            # =========================
            # XREAD
            # =========================
            elif cmd == "XREAD":

                lower_parts = [
                    x.lower() for x in command_parts
                ]

                streams_index = lower_parts.index(
                    "streams"
                )

                stream_key = command_parts[
                    streams_index + 1
                ]

                start_id = command_parts[
                    streams_index + 2
                ]

                # -------------------------
                # BLOCKING XREAD $
                # -------------------------
                if start_id == "$":

                    current_len = 0

                    if stream_key in store:
                        current_len = len(
                            store[stream_key]
                        )

                    while True:

                        if (
                            stream_key in store
                            and len(store[stream_key]) > current_len
                        ):

                            entry = store[
                                stream_key
                            ][-1]

                            entry_id = entry["id"]

                            field = entry["fields"][0]
                            value = entry["fields"][1]

                            response = (
                                f"*1\r\n"
                                f"*2\r\n"
                                f"${len(stream_key)}\r\n"
                                f"{stream_key}\r\n"
                                f"*1\r\n"
                                f"*2\r\n"
                                f"${len(entry_id)}\r\n"
                                f"{entry_id}\r\n"
                                f"*2\r\n"
                                f"${len(field)}\r\n"
                                f"{field}\r\n"
                                f"${len(value)}\r\n"
                                f"{value}\r\n"
                            )

                            client.send(
                                response.encode()
                            )

                            break

                        time.sleep(0.01)

                # -------------------------
                # NORMAL XREAD
                # -------------------------
                else:

                    if stream_key not in store:

                        client.send(
                            b"$-1\r\n"
                        )

                        continue

                    matching_entries = []

                    for entry in store[stream_key]:

                        entry_id = entry["id"]

                        if (
                            compare_ids(
                                entry_id,
                                start_id
                            ) > 0
                        ):

                            matching_entries.append(
                                entry
                            )

                    if len(matching_entries) == 0:

                        client.send(
                            b"$-1\r\n"
                        )

                        continue

                    response = (
                        f"*1\r\n"
                        f"*2\r\n"
                        f"${len(stream_key)}\r\n"
                        f"{stream_key}\r\n"
                        f"*{len(matching_entries)}\r\n"
                    )

                    for entry in matching_entries:

                        entry_id = entry["id"]

                        field = entry["fields"][0]
                        value = entry["fields"][1]

                        response += (
                            f"*2\r\n"
                            f"${len(entry_id)}\r\n"
                            f"{entry_id}\r\n"
                            f"*2\r\n"
                            f"${len(field)}\r\n"
                            f"{field}\r\n"
                            f"${len(value)}\r\n"
                            f"{value}\r\n"
                        )

                    client.send(
                        response.encode()
                    )

            # =========================
            # UNKNOWN COMMAND
            # =========================
            else:

                client.send(
                    b"-ERR unknown command\r\n"
                )

        except Exception as e:

            print("ERROR:", e)

            break

    client.close()


def main():

    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind(
        ("127.0.0.1", 6379)
    )

    server.listen(5)

    print("Server started...")

    while True:

        client, addr = server.accept()

        thread = threading.Thread(
            target=handle_client,
            args=(client,)
        )

        thread.start()


if __name__ == "__main__":
    main()
