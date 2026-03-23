$ports = @(5001, 5002, 5003)
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

Write-Host "Starting Node 1 on port 5001..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:PORT='5001'; `$env:NODE_ID='Node-1'; python -m uvicorn node:app --port 5001"

Write-Host "Starting Node 2 on port 5002..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:PORT='5002'; `$env:NODE_ID='Node-2'; python -m uvicorn node:app --port 5002"

Write-Host "Starting Node 3 on port 5003..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$env:PORT='5003'; `$env:NODE_ID='Node-3'; python -m uvicorn node:app --port 5003"

Write-Host "All 3 nodes are starting in separate windows!"
Write-Host "You can now run the 'test_system.py' script or test manually via http://localhost:5001/docs"
