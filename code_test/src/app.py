from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from kafka import KafkaProducer
import json
import logging

# Init fastapi
app = FastAPI(title="Mark's order management API")

# DB config
SQLALCHEMY_DATABASE_URL = "sqlite:///./orders.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'order_events'

try:
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
except Exception as e:
    logging.warning(f"Can't connect to Kafka cause: {str(e)}")
    producer = None


# DB model
class OrderModel(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    customer_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def to_dict(self):
        # fmt: off
        self.dict = {
            "id": self.id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "price": self.price,
            "customer_name": self.customer_name,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
        # fmt: on
        return self.dict


# Pydantic models
class OrderCreate(BaseModel):
    product_name: str = Field(..., min_length=1, max_length=100)
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)
    customer_name: str = Field(..., min_length=1, max_length=100)


class OrderResponse(BaseModel):
    id: int
    product_name: str
    quantity: int
    price: float
    customer_name: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        json_encoders = {datetime: lambda v: v.isoformat()}


# Create DB tables
Base.metadata.create_all(bind=engine)


# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Kafka producer
def publish_order_event(order_data: dict):
    if producer:
        try:
            producer.send(KAFKA_TOPIC, value=order_data)
            producer.flush()
        except Exception as e:
            logging.error(f"Failed to publish order event to Kafka cause: {str(e)}")


# API routes
@app.post("/orders/", response_model=OrderResponse)
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    try:
        # Create order
        db_order = OrderModel(product_name=order.product_name, quantity=order.quantity, price=order.price, customer_name=order.customer_name)
        db.add(db_order)
        db.commit()
        db.refresh(db_order)
        # Publish order event
        order_data = {
            "order_id": db_order.id,
            "product_name": db_order.product_name,
            "quantity": db_order.quantity,
            "price": db_order.price,
            "customer_name": db_order.customer_name,
            "created_at": db_order.created_at.isoformat(),
            "updated_at": db_order.updated_at.isoformat(),
        }
        publish_order_event(order_data)
        return OrderResponse(**db_order.to_dict())
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: int, db: Session = Depends(get_db)):
    db_order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return OrderResponse(**db_order.to_dict())


@app.get("/orders/", response_model=list[OrderResponse])
def list_orders(db: Session = Depends(get_db)):
    db_orders = db.query(OrderModel).all()
    return [OrderResponse(**order.to_dict()) for order in db_orders]


@app.put("/orders/{order_id}", response_model=OrderResponse)
def update_order(order_id: int, order: OrderCreate, db: Session = Depends(get_db)):
    db_order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    try:
        # 更新訂單資料
        db_order.product_name = order.product_name
        db_order.quantity = order.quantity
        db_order.price = order.price
        db_order.customer_name = order.customer_name

        db.commit()
        db.refresh(db_order)
        return OrderResponse(**db_order.to_dict())
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/orders/{order_id}")
def delete_order(order_id: int, db: Session = Depends(get_db)):
    db_order = db.query(OrderModel).filter(OrderModel.id == order_id).first()
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    try:
        db.delete(db_order)
        db.commit()
        return {"message": "Order deleted"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
