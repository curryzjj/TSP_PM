package System.tools;

import System.util.OsUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class PrecisionComputation {
    public static double precisionComputation(String FileDirectory, String application, int FTOption, int FailureTime) throws FileNotFoundException {
        String filePath = FileDirectory + OsUtils.osWrapperPostFix("Database")
                + OsUtils.osWrapperPostFix(application)
                + OsUtils.osWrapperPostFix(String.valueOf(FTOption))
                + OsUtils.osWrapperPostFix(String.valueOf(FailureTime));
        String basePath = FileDirectory + OsUtils.osWrapperPostFix("Database")
                + OsUtils.osWrapperPostFix(application)
                + OsUtils.osWrapperPostFix(String.valueOf(0))
                + OsUtils.osWrapperPostFix(String.valueOf(1));
        Scanner scanner = new Scanner(new File(filePath), "UTF-8");
        Scanner baseScanner = new Scanner(new File(basePath), "UTF-8");
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            totalNum ++;
            double baseString = Long.parseLong(baseScanner.nextLine().split(",")[1]);
            double compareString = Long.parseLong(scanner.nextLine().split(",")[1]);
            sum = sum + Math.abs(compareString - baseString) / baseString;
        }
        scanner.close();
        baseScanner.close();
        return sum / totalNum;
    }
}
