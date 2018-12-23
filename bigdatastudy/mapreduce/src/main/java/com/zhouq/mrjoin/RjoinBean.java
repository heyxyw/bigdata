package com.zhouq.mrjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zq on 2018/12/11.
 */
public class RjoinBean implements Writable {
    private int order_id;
    private String date_str;
    private String product_id;
    private int amount;
    private String produce_name;
    private float price;


    // 表示 是产品表  还是订单表
    // flag == 1 订单表 flag== 0 产品表
    private String flag;


    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public String getDate_str() {
        return date_str;
    }

    public void setDate_str(String date_str) {
        this.date_str = date_str;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProduce_name() {
        return produce_name;
    }

    public void setProduce_name(String produce_name) {
        this.produce_name = produce_name;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }


    public void set(int order_id, String date_str, String product_id, int amount, String produce_name, float price, String flag) {
        this.order_id = order_id;
        this.date_str = date_str;
        this.product_id = product_id;
        this.amount = amount;
        this.produce_name = produce_name;
        this.price = price;
        this.flag = flag;
    }

    /**
     * private int order_id;
     * private String date_str;
     * private String product_id;
     * private int amount;;
     * private String produce_name;
     * private float price;
     *
     * @param dataOutput
     * @throws IOException
     */

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeUTF(date_str);
        dataOutput.writeUTF(product_id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(produce_name);
        dataOutput.writeFloat(price);
        dataOutput.writeUTF(flag);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readInt();
        this.date_str = dataInput.readUTF();
        this.product_id = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.produce_name = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return order_id +
                "\t" + date_str +
                "\t" + product_id +
                "\t" + amount +
                "\t" + produce_name +
                "\t" + price
                ;
    }
}
