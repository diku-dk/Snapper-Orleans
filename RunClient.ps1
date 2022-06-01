# run controller
Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentController $numCPU $implementation $loggingEnabled"
Start-Sleep 20

# run worker
for ($workerID = 0; $workerID -le 8; $workerID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess $workerID"
}