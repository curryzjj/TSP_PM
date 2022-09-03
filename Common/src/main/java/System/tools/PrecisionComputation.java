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
        double errorNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            sum ++;
            String baseString = baseScanner.nextLine();
            String compareString = scanner.nextLine();
            if (!baseString.equals(compareString)) {
                errorNum ++;
            }
        }
        System.out.println(errorNum);
        System.out.println(sum);
        scanner.close();
        baseScanner.close();
        return (sum - errorNum) / sum;
    }
}
