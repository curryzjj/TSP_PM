package utils;

public interface SupplierWithException<R,E extends Throwable>{
    /**
     * Gets the result of this supplier.
     * @return the result of the supplier.
     * @throws E This function may throw an exception.
     */
    R get() throws E;
}
