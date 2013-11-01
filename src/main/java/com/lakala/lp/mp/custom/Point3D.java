/**
 * 项目名称：拉卡拉HADOOP实践
 * 包名：com.lakala.lp.mp
 * 文件名：Point3D.java
 * 版本信息：V1.0
 * 日期：2013年10月21日-下午4:29:43
 * 作者：litj
 * Copyright (c) 2013拉卡拉支付有限公司-版权所有
 */

package com.lakala.lp.mp.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 类名称：Point3D
 * 类描述：(三维空间的坐标点P(x,y,z)定制为一个数据类型Point3D)
 * 创建人：litj
 * 创建时间：2013年10月21日 下午4:29:43
 * 修改人：
 * 修改时间：2013年10月21日 下午4:29:43
 * 修改备注：
 * 
 * @version 1.0.0
 */

public class Point3D implements Writable, WritableComparable<Point3D> {

	private float x, y, z;

	public float getX() {
		return x;
	}

	public float getY() {
		return y;
	}

	public void setY(float y) {
		this.y = y;
	}

	public float getZ() {
		return z;
	}

	public void setZ(float z) {
		this.z = z;
	}

	public void setX(float x) {
		this.x = x;
	}

	public void readFields(DataInput in) throws IOException {
		x = in.readFloat();
		y = in.readFloat();
		z = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(x);
		out.writeFloat(y);
		out.writeFloat(z);
	}

	
	@Override
	public int compareTo(Point3D o) {
		return 0;
	}
}
