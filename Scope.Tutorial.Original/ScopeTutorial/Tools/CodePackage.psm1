
Set-StrictMode -Version 2 
$ErrorActionPreference = "Stop"

function Remove-FolderIfExists
{
	param
	(
		[parameter(Mandatory=$true)] [string] $Folder , 
		[parameter(Mandatory=$true)] [string] $Description 
	)
	process
	{
		if ( test-path $Folder)
		{
			Write-Verbose $Description
    		Remove-Item $Folder -Recurse
		}
	}
}

function Remove-FileIfExists
{
	param
	(
		[parameter(Mandatory=$true)] [string] $Filename , 
		[parameter(Mandatory=$true)] [string] $Description 
	)
	process
	{
		if ( test-path $Filename)
		{
			Write-Verbose $Description
    		Remove-Item $Filename -Recurse
		}
	}
}

function ZIPFolder
{
	param
	(
		[parameter(Mandatory=$true)] [string] $Folder , 
		[parameter(Mandatory=$true)] [string] $ZipFIle, 
		[parameter(Mandatory=$true)] [bool] $IncludeFolderInZip
	)
	process
	{
		Write-Verbose "ZIPFolder"
		Write-Verbose "Loading System.IO.Compression.FileSystem"
		$comp_asm = [Reflection.Assembly]::LoadWithPartialName( "System.IO.Compression.FileSystem" )
		$compressionLevel = [System.IO.Compression.CompressionLevel]::Optimal
		[System.IO.Compression.ZipFile]::CreateFromDirectory($Folder, $ZipFile, $compressionLevel, $IncludeFolderInZip )

		if (!(test-path $zipfile) )
		{
			Write-Host Failed to create ZIP file
			Exit
		}
	}
}

function Copy-CodeFolder
{
	param
	(
		[parameter(Mandatory=$true)] [string] $SourceFolder , 
		[parameter(Mandatory=$true)] [string] $OutputFolder
	)
	process
	{
	
		# ---------------------------------
		# COPY FILES TO THE STAGING FOLDER
		# Remove the read-only flag with /A-:R
		# Exclude Files with /XF option
		#  *.suo 
		#  *.user 
		#  *.vssscc 
		#  *.vspscc 
		# Exclude folders with /XD option
		#  bin
		#  obj
		#  _Resharper

		# Control verbosity 
		#  Don't show the names of files /NFL
		#  Don't show the names of directories /NDL
		&robocopy $SourceFolder $OutputFolder /MIR /A-:R /XF *.suo /XF *.user /XF *.vssscc /XF *.vspscc /XF *.ignore /XF *.temp /XF *.tmp /NFL /NDL /XD bin /XD obj /XD _ReSharper*
	}
}



function UnbindVSSourceControl
{
	param
	(
		[parameter(Mandatory=$true)] [string] $Folder
	)
	process
	{
		Write-Host "Unbind Source Control"
		if (!(test-path $Folder))
		{
			Write-Host ERROR $Folder does not exist
			break
			return;
		}

		function RemoveSCCElementsAttributes($el)
		{
			if ($el.Name.LocalName.StartsWith("Scc"))
			{
				# the the current element starts with Scc
				# Prune it and its children from the DOM
				$el.Remove();
				return;
			}
			else
			{
				# The current elemenent does not start with Scc
				# delete and Scc attributes it may have
				foreach ($attr in $el.Attributes())
				{
					if ($attr.Name.LocalName.StartsWith("Scc"))
					{
						$attr.Remove();
					}
				}

				# Check the children for any SCC Elements or attributes
				foreach ($child in $el.Elements())
				{
					RemoveSCCElementsAttributes($child);
				}
			}
		}

		Write-Host Unbinding SLN files from Source Control
		Write-Host $Folder
		$slnfiles = Get-ChildItem $Folder *.sln -Recurse

		foreach ($slnfile in $slnfiles) 
		{
			Write-Verbose $slnfile
			$insection = $false
			write-host $slnfile
			$input_lines = get-content $slnfile.FullName
			$output_lines = new-object 'System.Collections.Generic.List[string]'

			foreach ($line in $input_lines) 
			{
				$line_trimmed = $line.Trim()

				if ($line_trimmed.StartsWith("GlobalSection(SourceCodeControl)") -Or $line_trimmed.StartsWith("GlobalSection(TeamFoundationVersionControl)"))
				{
					Write-Verbose ("Removing: " +  $line_trimmed)
					$insection = $true	
					# do not copy this line to output
				}
				elseif ($line_trimmed.StartsWith("EndGlobalSection"))
				{
					$insection = $false
					# do not copy this line to output
				}
				elseif ($line_trimmed.StartsWith("Scc"))
				{
					# do not copy this line to output
				}
				else
				{
					if ( !($insection))
					{
						$output_lines.Add( $line )
					}
				}

			}
			$output_lines | Out-File $slnfile.FullName
		}


		# ---------------------------------
		# UNBIND PROJ FILES FROM SOURCE CONTROL
		Write-Host Unbinding PROJ files from Source Control
		$projfiles = Get-ChildItem $Folder *.*proj -Recurse
		[Reflection.Assembly]::LoadWithPartialName("System.Xml.Linq") | Out-Null
		foreach ($projfile in $projfiles) 
		{
			$doc = [System.Xml.Linq.XDocument]::Load( $projfile.FullName )
			RemoveSCCElementsAttributes($doc.Root);
			$doc.Save( $projfile.FullName )
		}
	}
}

