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
