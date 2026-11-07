"""Manages the Mosquitto logins for the cold-chain project (run it from the coldchain folder).

  python mqtt_users.py init             create passwd.txt with the 'agent' and 'nodered' logins
                                        (also wires the passwords into start_agent.bat and .env)
  python mqtt_users.py add <tenant-id>  create, or reset, the login of one company's devices
  python mqtt_users.py list             show which logins exist (names only, never passwords)
  python mqtt_users.py nodered-login    tell the running Node-RED to use its login (once, after init)

Passwords are random and are stored only as hashes in passwd.txt.
Mosquitto reads passwd.txt when it starts, so restart Mosquitto after init or add.
"""
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import urllib.error
import urllib.request

PASSWD_FILE = "passwd.txt"
ENV_FILE = ".env"
BAT_FILE = "start_agent.bat"
NODERED_URL = os.environ.get("NODERED_URL", "http://localhost:1880")
RESERVED = {"agent", "nodered"}


def find_mosquitto_passwd():
    candidates = [
        os.environ.get("MOSQUITTO_PASSWD", ""),
        shutil.which("mosquitto_passwd") or "",
        r"C:\Program Files\mosquitto\mosquitto_passwd.exe",
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    sys.exit("Cannot find mosquitto_passwd. It is installed with Mosquitto, normally in "
             r"C:\Program Files\mosquitto\ . Set MOSQUITTO_PASSWD to its full path.")


def set_password(user, password):
    tool = find_mosquitto_passwd()
    cmd = [tool, "-b", PASSWD_FILE, user, password]
    if not os.path.exists(PASSWD_FILE):
        cmd.insert(1, "-c")          # -c = create the file
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit("mosquitto_passwd failed: " + (result.stderr or result.stdout).strip())


def new_password():
    return secrets.token_hex(12)     # 24 hex characters: random, and safe inside commands


def read_text(path):
    with open(path, newline="") as f:
        return f.read()


def write_text(path, text):
    with open(path, "w", newline="") as f:
        f.write(text)


def set_env_key(path, key, value):
    """Set KEY=value in a .env file, keeping every other line (and its line endings) as it was."""
    text = read_text(path) if os.path.exists(path) else ""
    eol = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()
    new_line = f"{key}={value}"
    for i, line in enumerate(lines):
        if line.startswith(key + "="):
            lines[i] = new_line
            break
    else:
        lines.append(new_line)
    write_text(path, eol.join(lines) + eol)


def set_agent_login_in_bat(path, user, password):
    """Put 'set MQTT_USER=...' and 'set MQTT_PASSWORD=...' in start_agent.bat, just before the uvicorn line."""
    text = read_text(path) if os.path.exists(path) else ""
    eol = "\r\n"
    lines = [l for l in text.splitlines()
             if not re.match(r"\s*set\s+MQTT_(USER|PASSWORD)=", l, re.IGNORECASE)]
    new = [f"set MQTT_USER={user}", f"set MQTT_PASSWORD={password}"]
    for i, line in enumerate(lines):
        if "uvicorn" in line:
            lines[i:i] = new
            break
    else:
        lines = new + lines
    write_text(path, eol.join(lines) + eol)


def cmd_init():
    agent_pw, nodered_pw = new_password(), new_password()
    set_password("agent", agent_pw)
    set_password("nodered", nodered_pw)
    set_env_key(ENV_FILE, "MQTT_NODERED_PASSWORD", nodered_pw)
    set_agent_login_in_bat(BAT_FILE, "agent", agent_pw)
    print(f"Created {PASSWD_FILE} with the logins 'agent' and 'nodered'.")
    print(f"  - the agent's login was written into {BAT_FILE}")
    print(f"  - Node-RED's password was written into {ENV_FILE} (docker compose reads it from there)")
    print("Next: restart Mosquitto, restart the agent with start_agent.bat, then recreate Node-RED.")


def cmd_add(tenant):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", tenant):
        sys.exit("A tenant id may only contain letters, digits, '-' and '_' (a UUID is fine).")
    if tenant in RESERVED:
        sys.exit(f"'{tenant}' is reserved for the platform itself.")
    pw = new_password()
    set_password(tenant, pw)
    print(f"Login for company {tenant}:")
    print(f"  username : {tenant}      (it is the company's tenant id, nothing to configure)")
    print(f"  password : {pw}")
    print("This is the only time the password is shown. Put it in the device (secrets.h),")
    print("or for the fake sensor:   set DEVICE_PASSWORD=" + pw + "   (Command Prompt)")
    print("Restart Mosquitto so it picks up the new login.")


def cmd_list():
    if not os.path.exists(PASSWD_FILE):
        sys.exit(f"{PASSWD_FILE} does not exist yet. Run: python mqtt_users.py init")
    for line in read_text(PASSWD_FILE).splitlines():
        if ":" in line:
            print(line.split(":", 1)[0])


def cmd_nodered_login():
    """Equivalent of typing ${MQTT_USER} / ${MQTT_PASSWORD} into the broker's Security tab and pressing Deploy."""
    try:
        with urllib.request.urlopen(NODERED_URL + "/flows", timeout=10) as r:
            flows = json.load(r)
        changed = 0
        for node in flows:
            if node.get("type") == "mqtt-broker":
                node["credentials"] = {"user": "${MQTT_USER}", "password": "${MQTT_PASSWORD}"}
                changed += 1
        if not changed:
            sys.exit("No MQTT broker node found in Node-RED's flow.")
        req = urllib.request.Request(
            NODERED_URL + "/flows", data=json.dumps(flows).encode(), method="POST",
            headers={"Content-Type": "application/json", "Node-RED-Deployment-Type": "full"})
        urllib.request.urlopen(req, timeout=10).read()
    except urllib.error.URLError as e:
        sys.exit(f"Cannot reach Node-RED at {NODERED_URL} ({e.reason}). Is the container running?")
    print("Node-RED will now log in with MQTT_USER / MQTT_PASSWORD from its environment.")
    print("Check the log: docker compose -f docker-compose.nodered.yml logs --tail 5 nodered")


def main():
    args = sys.argv[1:]
    if args == ["init"]:
        cmd_init()
    elif len(args) == 2 and args[0] == "add":
        cmd_add(args[1])
    elif args == ["list"]:
        cmd_list()
    elif args == ["nodered-login"]:
        cmd_nodered_login()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
