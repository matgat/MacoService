## [MacoService](https://github.com/matgat/MacoService.git)

Release of MacoService folder apps for testing/emulation purposes.

First time:

```bat
C:\> mkdir Macotec
C:\> cd Macotec
C:\Macotec> git clone https://github.com/matgat/MacoService.git
```
Or, to clone a certain branch:

```
C:\Macotec> git clone -b monitoring https://github.com/matgat/MacoService.git
```

> [!TIP]
> To install [git](https://git-scm.com) on Windows:
>
> ```bat
> > winget install Git.Git
> > winget install TortoiseGit.TortoiseGit
> ```

> [!TIP]
> To switch and track a certain (new) branch:
>
> ```bat
> C:\Macotec\MacoService> git switch --track origin/monitoring
> ```

> [!TIP]
> To update the content from remote:
> 
> ```bat
> C:\Macotec\MacoService> git pull
> ```


### Emulating a work (strato machine)

1. Launch MacoLayer.exe (needs a win32 compatible environment)
   - If a firewall is present, add an exception
   - If problems occur, set compatibility options to
     “Run this program in compatibility mode for Windows 7”

2. Simulate a process
   - Open the main window (double-click the **MacoLayer** tray icon)
   - Click the **"Status"** tab
   - Open the context menu for the **"ActiveW"** node and click **"Test board"**
   - In the window that appears:
       * Right-click on the left sidebar
       * Click **"Send Strato Project"**
       * Press **"OK"** in the dialog that pops up
   - Click the green button to start the job on the emulated machine

3. Close the program
   - Focus the **MacoLayer** main window
   - Press `ALT+F4`


### Monitoring
To monitor the machine, see the example script `machine-monitoring.py`
versioned in the `monitoring` branch.
This script shows how to connect to the machine and respond to its
status changes.
The function `publish_data()` is called when one or more fields change value;
put your custom actions there to consume the data (import any libraries you
need and do what you want: write to a socket or file, send an email, notify
a supervisor, etc.).
For the meaning of the various fields, refer to the file `Interface.xml`.
To automate launching the script, use *scheduled tasks* to run it at boot.

Edit and customize the script

* Edit `custom_convert()` to filter, modify, or rename incoming fields as needed.
* Add your custom actions to `publish_data()` to consume the incoming values.


Here's an example of a float machine session,
filtering the axes position changes:

```json
{"mach-status":"ready","op-status":"none","scheme-done":0,"can-receive-job":1,"working":0,"emg-list":"","msg-list":"","scheme":0,"job-loaded":0,"status-lights":["air-ok","power-on","parked","may-enter"],"work-selectors":["enab-tools","enab-lowe","cut-lub","enab-labels"],"mode":"manual","spdovd-lowe":100,"spdovd":100,"ebrk-scheme":0,"ebrk-stripe":0,"ebrk-scheme-done":0,"probe-status":0,"scheme-progress":0,"step":0}

{"scheme":1,"job-loaded":1,"status-lights":["air-ok","power-on","parked","ready-to-work","may-enter"],"mode":"auto"}

{"mach-status":"working","op-status":"scoring","can-receive-job":0,"working":1,"status-lights":["air-ok","power-on"],"scheme-progress":0,"step":0}

{"mach-status":"stopping","op-status":"none","can-receive-job":1,"working":0,"mode":"manual"}

{"mach-status":"ready","status-lights":["air-ok","power-on","parked","ready-to-work","may-enter"]}

{"mach-status":"error","emg-list":[123,30800012],"status-lights":["cnc-error","may-enter"]}
```
