package System.tools;

import System.util.OsUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class PrecisionComputation {
    public static double stateDegradation(String FileDirectory, String application, int FTOption, int FailureTime) throws FileNotFoundException {
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
    public static double relativeError(String FileDirectory, String application, int FTOption, int FailureTime) throws FileNotFoundException {
        String filePath = FileDirectory + OsUtils.osWrapperPostFix("OutputResult")
                + OsUtils.osWrapperPostFix(application)
                + OsUtils.osWrapperPostFix(String.valueOf(FTOption))
                + OsUtils.osWrapperPostFix(String.valueOf(FailureTime));
        String basePath = FileDirectory + OsUtils.osWrapperPostFix("OutputResult")
                + OsUtils.osWrapperPostFix(application)
                + OsUtils.osWrapperPostFix(String.valueOf(0))
                + OsUtils.osWrapperPostFix(String.valueOf(0));
        Scanner scanner = new Scanner(new File(filePath), "UTF-8");
        Scanner baseScanner = new Scanner(new File(basePath), "UTF-8");
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            totalNum ++;
            double resultDegradation = 0;
            String[] baseResults = baseScanner.nextLine().split(";")[1].split(",");
            String[] compareResults = scanner.nextLine().split(";")[1].split(",");
            for (int i = 0; i < baseResults.length; i++) {
                if (Double.parseDouble(baseResults[i]) == 0){
                    resultDegradation = resultDegradation
                            + Math.abs(Double.parseDouble(compareResults[i]) - Double.parseDouble(baseResults[i])) / (Double.parseDouble(baseResults[i]) + 1);
                } else {
                    resultDegradation = resultDegradation
                            + Math.abs(Double.parseDouble(compareResults[i]) - Double.parseDouble(baseResults[i])) / (Double.parseDouble(baseResults[i]));
                }
            }
            sum = sum + resultDegradation;
        }
        scanner.close();
        baseScanner.close();
        return sum / totalNum;
    }
    public static double OBApplication(Scanner scanner, Scanner baseScanner) {
        double totalNum = 0;
        double sum = 0;
        while (baseScanner.hasNextLine()) {
            String baseResult = baseScanner.nextLine();
            String compareResult = scanner.nextLine();
            totalNum ++;
            double priceDegradation;
            double basePrice = Long.parseLong(baseResult.split(",")[1].trim());
            double comparePrice = Long.parseLong(compareResult.split(",")[1].trim());
            if (basePrice == 0) {
                priceDegradation = Math.abs(basePrice - comparePrice) / (basePrice + 1);
            } else {
                priceDegradation = Math.abs(basePrice - comparePrice) / basePrice;
            }
            double qtyDegradation;
            double baseQty = Long.parseLong(baseResult.split(",")[2]);
            double compareQty = Long.parseLong(compareResult.split(",")[2]);
            if (baseQty == 0) {
                qtyDegradation = Math.abs(baseQty - compareQty) / (baseQty + 1);
            } else {
                qtyDegradation = Math.abs(baseQty - compareQty) / baseQty;
            }
            sum = sum + (priceDegradation + qtyDegradation) / 2;
        }
        scanner.close();
        baseScanner.close();
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
                double baseSpeed = Double.parseDouble(baseString[1]);
                double compareSpeed = Double.parseDouble(compareString[1]);
                Degradation = Math.abs(baseSpeed - compareSpeed) / baseSpeed;
            }
            sum =  sum + Degradation;
        }
        scanner.close();
        baseScanner.close();
        return sum / totalNum;
    }
}
