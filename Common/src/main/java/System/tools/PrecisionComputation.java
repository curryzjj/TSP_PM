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
                + OsUtils.osWrapperPostFix(String.valueOf(0));
        Scanner scanner = new Scanner(new File(filePath), "UTF-8");
        Scanner baseScanner = new Scanner(new File(basePath), "UTF-8");
        switch (application) {
            case "GS_txn" :
                return GSApplication(scanner, baseScanner);
            case "SL_txn" :
                return SLApplication(scanner, baseScanner);
            case "OB_txn" :
                return OBApplication(scanner, baseScanner);
            case "TP_txn" :
                return TPApplication(scanner, baseScanner);
            default:
                return 0.0;
        }
    }
    public static double OBApplication(Scanner scanner, Scanner baseScanner) {
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            totalNum ++;
            double priceDegradation;
            double basePrice = Long.parseLong(baseScanner.nextLine().split(",")[1].trim());
            double comparePrice = Long.parseLong(scanner.nextLine().split(",")[1].trim());
            if (basePrice == 0) {
                priceDegradation = Math.abs(basePrice - comparePrice) / (basePrice + 1);
            } else {
                priceDegradation = Math.abs(basePrice - comparePrice) / basePrice;
            }
            double qtyDegradation;
            double baseQty = Long.parseLong(baseScanner.nextLine().split(",")[2]);
            double compareQty = Long.parseLong(scanner.nextLine().split(",")[2]);
            if (baseQty == 0) {
                qtyDegradation = Math.abs(baseQty - compareQty) / (baseQty + 1);
            } else {
                qtyDegradation = Math.abs(baseQty - compareQty) / baseQty;
            }
            sum = sum + (priceDegradation + qtyDegradation) / 2;
        }
        return sum / totalNum;
    }
    public static double SLApplication(Scanner scanner, Scanner baseScanner) {
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
    public static double GSApplication(Scanner scanner, Scanner baseScanner) {
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            totalNum ++;
            double baseString = Long.parseLong(baseScanner.nextLine().split(",")[1].trim());
            double compareString = Long.parseLong(scanner.nextLine().split(",")[1].trim());
            if (baseString == 0) {
                sum = sum + Math.abs(compareString - baseString) / (baseString + 1);
            } else {
                sum = sum + Math.abs(compareString - baseString) / baseString;
            }
        }
        scanner.close();
        baseScanner.close();
        return sum / totalNum;
    }
    public static double TPApplication(Scanner scanner, Scanner baseScanner) {
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            totalNum ++;
            double Degradation;
            String[] baseString = baseScanner.nextLine().split(",");
            String[] compareString = scanner.nextLine().split(",");
            if (baseString[1].split(" ").length > 1) {
                double baseCnt = baseString[1].split(" ").length;
                double compareCnt = compareString[1].split(" ").length;
                Degradation = Math.abs(baseCnt - compareCnt) / baseCnt;
            } else {
                double baseSpeed = Double.parseDouble(baseScanner.nextLine().split(",")[1].trim());
                double compareSpeed = Double.parseDouble(scanner.nextLine().split(",")[1].trim());
                Degradation = Math.abs(baseSpeed - compareSpeed) / baseSpeed;
            }
            sum =  sum + Degradation;
        }
        return sum / totalNum;
    }
}
