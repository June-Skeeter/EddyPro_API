cd c:\MM_Py\EddyPro_API/temp/26156/bin/
START /HIGH eddypro_rp.exe
WMIC process where name="eddypro_rp.exe" CALL setpriority "high"
START eddypro_fcc.exe
EXIT