$ErrorActionPreference = "Stop"

$version = "3.3.0-1.20"
$filename = "flink-sql-connector-kafka-$version.jar"
$expectedSha1 = "9e7e2bb762e6cb489bcc76f2637e824fbb6f08c3"
$destinationDirectory = Join-Path $PSScriptRoot "..\.flink\lib"
$destination = Join-Path $destinationDirectory $filename
$downloadUrl = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/$version/$filename"

New-Item -ItemType Directory -Force -Path $destinationDirectory | Out-Null
if (-not (Test-Path -LiteralPath $destination)) {
    Invoke-WebRequest -Uri $downloadUrl -OutFile $destination
}

$actualSha1 = (Get-FileHash -LiteralPath $destination -Algorithm SHA1).Hash.ToLowerInvariant()
if ($actualSha1 -ne $expectedSha1) {
    throw "Kafka connector checksum mismatch. Delete '$destination' and retry."
}

Write-Output "Installed verified Flink Kafka connector: $destination"
