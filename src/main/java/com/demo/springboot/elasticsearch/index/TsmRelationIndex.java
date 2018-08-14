package com.demo.springboot.elasticsearch.index;

public class TsmRelationIndex {
    private int id;
    private String supplierInterid;
    private int ouId;
    private String ouCode;
    private String ouName;
    private String productCode;
    private String productName;
    private int manageGroupId;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getSupplierInterid() {
        return supplierInterid;
    }

    public void setSupplierInterid(String supplierInterid) {
        this.supplierInterid = supplierInterid;
    }

    public int getOuId() {
        return ouId;
    }

    public void setOuId(int ouId) {
        this.ouId = ouId;
    }

    public String getOuCode() {
        return ouCode;
    }

    public void setOuCode(String ouCode) {
        this.ouCode = ouCode;
    }

    public String getOuName() {
        return ouName;
    }

    public void setOuName(String ouName) {
        this.ouName = ouName;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getManageGroupId() {
        return manageGroupId;
    }

    public void setManageGroupId(int manageGroupId) {
        this.manageGroupId = manageGroupId;
    }
}
