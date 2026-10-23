# Apache Artemis 2.54.0 Setup Guide (Ubuntu 24.04)

This README provides a step-by-step explanation of the commands used to install Apache Artemis and details the specific configuration changes made to the broker files.

## 1. Installation Commands

The following commands prepare the Ubuntu 24.04 environment, download the binaries, and initialize the Artemis broker:

* `apt install nano`
    Installs the `nano` command-line text editor, which is used to edit the configuration files later.

* `sudo apt install openjdk-21-jdk`
    Installs the Java 21 Development Kit. Apache Artemis runs on the Java Virtual Machine (JVM), making this a required dependency.

* `wget -O apache-artemis-2.54.0-bin.tar.gz "https://www.apache.org/dyn/closer.lua?filename=artemis/artemis/2.54.0/apache-artemis-2.54.0-bin.tar.gz&action=download"`
    Downloads the Artemis 2.54.0 binary release from the official Apache mirrors and saves it locally as `apache-artemis-2.54.0-bin.tar.gz`.

* `tar -xzf apache-artemis-2.54.0-bin.tar.gz`
    Extracts the downloaded compressed tarball, creating a directory named `apache-artemis-2.54.0` containing the compiled Artemis binaries.

* `sudo apache-artemis-2.54.0/bin/artemis create /var/lib/mybroker`
    Uses the Artemis CLI tool to create a new broker instance located at `/var/lib/mybroker`. This generates the necessary folder structure, including the `etc/` folder where your configuration files live.

## 2. Configuration File Edits

After creating the broker, three critical configuration files located in `/var/lib/mybroker/etc/` were edited. Here is exactly what was modified in each file and why:

### `broker.xml`
This is the core configuration file that dictates how the messaging broker behaves.
* **External Network Bindings:** Under the `<acceptors>` block, the IP addresses for all protocols (`artemis`, `amqp`, `stomp`, `hornetq`, `mqtt`) were changed to bind to `tcp://0.0.0.0:<port>`. By default, they bind to localhost. Changing them to `0.0.0.0` allows the broker to accept messaging connections from external machines.
* **Auto-delete Disabled:** In the default `<address-setting match="#">` block, both `auto-delete-queues` and `auto-delete-addresses` were set to `false`. This prevents the broker from automatically cleaning up and deleting queues when they are empty and have no active consumers.
* **SnapshotQueue Expiry Policy:** A new address setting `<address-setting match="SnapshotQueue">` was added. It configures messages in `SnapshotQueue` to expire between 0 and 240,000 milliseconds (4 minutes). Once a message expires, it is automatically moved to the `WorkQueue` (`<expiry-address>WorkQueue</expiry-address>`).
* **Custom Addresses & Last-Value Queues:** Added explicit definitions for `WorkQueue` and `SnapshotQueue` under the `<addresses>` block. Both are configured with `last-value="true"`. A Last-Value Queue acts like a specialized cache: it only retains the most recent message for a given property key, discarding any older messages that share the same key.

### `bootstrap.xml`
This file controls the embedded web server hosting the Artemis Management Console.
* **Web Console Binding:** The binding URI was changed from `localhost` to `http://0.0.0.0:8161` (`<binding name="artemis" uri="http://0.0.0.0:8161">`). This makes the graphical web management console accessible from external web browsers, rather than just locally on the server.

### `jolokia-access.xml`
This file configures security for Jolokia, the JMX-to-HTTP bridge used by the web management console to communicate with the broker.
* **CORS Relaxed:** Added `<allow-origin>*://0.0.0.0*</allow-origin>` and `<allow-origin>*</allow-origin>`. This fixes Cross-Origin Resource Sharing (CORS) restrictions, allowing external web clients to successfully hit the management API.
* **Ignore Scheme:** Uncommented the `<ignore-scheme/>` tag. This tells Jolokia to discard strict HTTPS origin checks, which is necessary if you are accessing the management console externally, especially if traffic is passing through proxies or docker networks that transform requests.

## 3. Starting the Broker

* `sudo /var/lib/mybroker/bin/artemis.service start`
    Starts the Artemis broker as a background service using the auto-generated service script.
* *(Note on startup)*: To start the broker automatically on system boot, adding it to `~/.bashrc` works for user logins, but for a true headless server, you should link this service file to `systemd` (e.g., `sudo systemctl enable /var/lib/mybroker/bin/artemis.service`).
