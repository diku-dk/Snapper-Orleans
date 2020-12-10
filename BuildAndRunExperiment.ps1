# First build the Orleans vNext nuget packages locally
if((Test-Path "..\..\vNext\Binaries\Debug\") -eq $false) {
     # this will only work in Windows.
     # Alternatively build the nuget packages and place them in the <root>/vNext/Binaries/Debug folder
     # (or make sure there is a package source available with the Orleans 2.0 TP nugets)
    #..\..\Build.cmd netstandard
}

# Uncomment the following to clear the nuget cache if rebuilding the packages doesn't seem to take effect.
#dotnet nuget locals all --clear

dotnet restore
if ($LastExitCode -ne 0) { return; }

dotnet build --no-restore
if ($LastExitCode -ne 0) { return; }

$zipF = 1.0
$percent = 100, 90, 75, 50, 25, 0
$detPipe = 2048
$nonDetPipe = 4
$cpu = 4

$hotGrainRatio = 0.5  # 0.25, 0.5
$skewness = 0, 0.1, 0.05, 0.01, 0.005, 0.001

for ($j = 0; $j -le 0; $j++)
{
    $skew = $skewness[$j]
    for ($i = 0; $i -le 0; $i++)
    {
        $detPercent = $percent[$i]
        for ($pipe = 512; $pipe -le 512; $pipe *= 2)
        {
            $detPipe = $pipe
            #$nonDetPipe = $pipe
            Start-Process "dotnet" -ArgumentList "run --project ExperimentController --no-build -- $zipF $detPercent $cpu"
            Start-Sleep 8
            $numWorker = $cpu / 4 - 1;
            for ($worker = 0; $worker -le 0; $worker++)
            {
                #Start-Process "dotnet" -ArgumentList "run --project NewProcess --no-build -- $cpu $detPipe $nonDetPipe $skew $hotGrainRatio"
                #Start-Sleep 20
            }
            #Start-Process "dotnet" -ArgumentList "run --project ExperimentProcess --no-build -- $pipe $skew $hotGrainRatio"
            Start-Sleep 62
        }
    }
}
