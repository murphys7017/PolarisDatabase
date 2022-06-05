package polaris.db.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import polaris.db.backend.dm.DataManager;
import polaris.db.backend.server.Server;
import polaris.db.backend.tbm.TableManager;
import polaris.db.backend.tm.TransactionManager;
import polaris.db.backend.tm.TransactionManagerImpl;
import polaris.db.backend.utils.Panic;
import polaris.db.backend.vm.VersionManager;
import polaris.db.backend.vm.VersionManagerImpl;
import polaris.db.Error;

import java.io.File;

public class Launcher {

    public static final String DB_PATH = "F:\\IDEA\\PolarisDB";
    public static final String DB_NAME = "mydb";
    public static final String MEM = "64MB";


    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {


        File dbfile = new File(DB_PATH+File.separator+DB_NAME+".xid");
        if (dbfile.exists()){
            openDB(DB_PATH+File.separator+DB_NAME, parseMem(MEM));
            System.out.println("Usage: launcher open DBPath");
            return;
        }else {
            File path = new File(DB_PATH);
            path.mkdir();
            createDB(DB_PATH+File.separator+DB_NAME);
            System.out.println("Usage: launcher create DBPath");
            openDB(DB_PATH+File.separator+DB_NAME, parseMem(MEM));
            System.out.println("Usage: launcher open DBPath");
            return;
        }

    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
