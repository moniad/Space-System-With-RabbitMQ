public enum ServiceType {
    PEOPLE(1, "people"), LOAD(2, "load"), SATELLITE(3, "satellite");

    public int serviceTypeNumber;
    public String name;

    ServiceType(int number, String name) {
        serviceTypeNumber = number;
        this.name = name;
    }

    public static ServiceType of(int serviceTypeNumber) {
        ServiceType serviceType = values()[serviceTypeNumber - 1];
        if (serviceType == null) {
            throw new IllegalArgumentException("Invalid type number: " + serviceTypeNumber);
        }
        return serviceType;
    }
}
