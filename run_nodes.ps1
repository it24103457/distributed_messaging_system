$ports = @(5001, 5002, 5003, 5004, 5005)
foreach ($p in $ports) {
    Write-Host "Checking for existing servers on port $p..."
    $conns = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    if ($conns) {
        Write-Host "Killing background process $($conns.OwningProcess) on port $p..."
        Stop-Process -Id $conns.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "Checking/Installing dependencies..."
python -m pip install fastapi uvicorn requests pydantic

for ($i=1; $i -le 5; $i++) {
    $p = 5000 + $i
    Write-Host "Starting Node $i on port $p..."
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:PORT='$p'; `$env:NODE_ID='Node-$i'; python -m uvicorn node:app --port $p"
}

Write-Host "All 5 nodes are starting in separate windows!"
Write-Host "You can now run the 'test_system.py' script or test manually via http://localhost:5001/docs"
