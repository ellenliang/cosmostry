SET SCRIPTPATH=%~dp0
SET SAMPLESROOT=%~dp0..\
SET VCROOT=%~dp0VCROOT\
SET DRIVEROOT=%~d0
SET SCOPESDK=%~d0\ScopeSDK
SET SCOPEEXE=%~d0\ScopeSDK\scope.exe
@ECHO VCROOT is %VCROOT%
REM -RESOURCE_PATH %SCRIPTPATH%;%VCROOT%
%SCOPEEXE% run -i %1 %2 %3 %4 %5 %6 %7 %8 %9 -INPUT_PATH %VCROOT% -OUTPUT_PATH %VCROOT% -workingRoot %TEMP%   

    
