Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

# -------------------------------------------------

$REPO="http://vstfcodebox:8080/tfs/Eta/_git/CosmosSamples"
$filshare_location = "\\fsu\shares\cosmosfiles\CosmosSamples"
$GITEXE = "c:\Program Files (x86)\Git\bin\git.exe"
$DESTDIR = "d:\CosmosSamplesExport"
$zipfile = "d:\CosmosSamples.zip"

Resolve-Path $GITEXE
Resolve-Path $filshare_location

# -------------------------------------------------

function remove-folder ($p)
{
    if (Test-Path $p)
    {
        Write-Host Cleaning Out $p -ForegroundColor Green
        Remove-Item -Recurse -Force $p
    }
}

function remove-file ($p)
{`
    if (Test-Path $p)
    {
        Remove-Item -Force $p
    }
}

function clean-gitstuff( $p )
{
    Write-Host Cleaning Git stuff from folder
    Remove-Item -Recurse -Force ( join-path $p ".git")
    Remove-Item -Force ( join-path $p ".gitattributes")
    Remove-Item -Force ( join-path $p ".gitignore")
}

$asm = [Reflection.Assembly]::LoadWithPartialName( "System.IO.Compression.FileSystem" )

function zip-folder( $p , $z)
{
    $compressionLevel = [System.IO.Compression.CompressionLevel]::Optimal
    $includebasedir = $false

    Remove-File $z
    [System.IO.Compression.ZipFile]::CreateFromDirectory( $p, $z, $compressionLevel, $includebasedir ) 
}


# -------------------------------------------------

# CLEAN OUT TO DESTINATION FOLDER
remove-folder $DESTDIR

# CLONE FROM REPO INTO FOLDER
&$GITEXE clone --depth 1 $REPO $DESTDIR

# REMOVE GIT STUFF FROM THE DESTINATION FOLDER
clean-gitstuff $DESTDIR 

# ZIP IT UP
zip-folder $DESTDIR $zipfile

# COPY TO FILESHARE
COPY $zipfile $filshare_location
