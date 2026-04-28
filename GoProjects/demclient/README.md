# Config via env vars or flags

```bash
export SML_SERVER=http://localhost:8080
export SML_KEY_ID=user1
export SML_SECRET_KEY=base64secret
```

# Commands
```bash
demcli list                                           # list all projects
demcli create <project> [--type cpu] [--persist]      # create project
demcli info <project>                                 # project info
demcli stream <project> <file.csv>                    # stream CSV to 
project
demcli stream <project> --stdin                       # stream from stdin 
(pipe from otelsml)
demcli explore <project> <variable>                   # univariate stats
demcli explore <project> <var1> <var2>                # bivariate stats
demcli histogram <project> <variable> [--bins 20]     # density estimation
```