package engine.Exception;

public class SchemaException extends Exception {
    private String message;

    public SchemaException(String message) {
        this.message = message;
    }

    public SchemaException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
