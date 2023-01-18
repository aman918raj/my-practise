Spark, Hive practise code on local Windows machine.

1. place hadoop.dll and winutils.exe in <path>/hadoop/bin
2. set hadoop.home.dir to <path>/hadoop
3. place hadoop.dll file in C:/Windows/System32 directory
4. we also need to run:
   <path>/hadoop/bin/winutils.exe chmod 777 /tmp/hive
   
Note: If you are using office laptop then you need to be on VPN