"""
FastAPI web application for the RDBMS.
Provides specific CRUD endpoints for each table with Swagger UI documentation.
"""

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import sys
import os

# Add parent directory to path to import db modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.engine import DatabaseEngine
from db.storage import Storage
from db.parser import SQLParser

# Import models
from web.models import (
    UserCreate, UserUpdate, UserResponse,
    ProductCreate, ProductUpdate, ProductResponse,
    OrderCreate, OrderUpdate, OrderResponse,
    OrderItemCreate, OrderItemUpdate, OrderItemResponse,
    MessageResponse, CountResponse
)

# Initialize database engine
storage = Storage("data")
engine = DatabaseEngine(storage)
parser = SQLParser()

app = FastAPI(
    title="Pesapal RDBMS API",
    description="A simple relational database management system with CRUD operations",
    version="1.0.0"
)


# ============================================================================
# Users Endpoints
# ============================================================================

@app.get("/users", response_model=List[UserResponse], tags=["Users"])
async def get_all_users():
    """Get all users."""
    try:
        rows = engine.select("users", columns=None, where_clause=None)
        return [UserResponse(**row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/users/{user_id}", response_model=UserResponse, tags=["Users"])
async def get_user(user_id: int = Path(..., description="User ID")):
    """Get a specific user by ID."""
    try:
        rows = engine.select("users", columns=None, where_clause={"id": user_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        return UserResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/users", response_model=UserResponse, status_code=201, tags=["Users"])
async def create_user(user: UserCreate):
    """Create a new user."""
    try:
        # Get current max ID to find the new user
        all_users = engine.select("users", columns=None, where_clause=None)
        max_id = max([u["id"] for u in all_users]) if all_users else 0
        
        # Insert user (id will be auto-incremented)
        engine.insert("users", [None, user.name, user.email, user.age])
        
        # Get the created user (should have ID = max_id + 1)
        rows = engine.select("users", columns=None, where_clause={"email": user.email})
        if rows:
            return UserResponse(**rows[0])
        raise HTTPException(status_code=500, detail="Failed to retrieve created user")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/users/{user_id}", response_model=UserResponse, tags=["Users"])
async def update_user(user_id: int, user: UserUpdate):
    """Update a user by ID."""
    try:
        # Check if user exists
        rows = engine.select("users", columns=None, where_clause={"id": user_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        # Build update assignments (only include non-None values)
        assignments = {}
        if user.name is not None:
            assignments["name"] = user.name
        if user.email is not None:
            assignments["email"] = user.email
        if user.age is not None:
            assignments["age"] = user.age
        
        if assignments:
            engine.update("users", assignments, {"id": user_id})
        
        # Get updated user
        rows = engine.select("users", columns=None, where_clause={"id": user_id})
        return UserResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/users/{user_id}", response_model=MessageResponse, tags=["Users"])
async def delete_user(user_id: int):
    """Delete a user by ID."""
    try:
        rows_deleted = engine.delete("users", {"id": user_id})
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        return MessageResponse(message=f"User with ID {user_id} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Products Endpoints
# ============================================================================

@app.get("/products", response_model=List[ProductResponse], tags=["Products"])
async def get_all_products():
    """Get all products."""
    try:
        rows = engine.select("products", columns=None, where_clause=None)
        return [ProductResponse(**row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/products/{product_id}", response_model=ProductResponse, tags=["Products"])
async def get_product(product_id: int = Path(..., description="Product ID")):
    """Get a specific product by ID."""
    try:
        rows = engine.select("products", columns=None, where_clause={"id": product_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        return ProductResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/products", response_model=ProductResponse, status_code=201, tags=["Products"])
async def create_product(product: ProductCreate):
    """Create a new product."""
    try:
        engine.insert("products", [None, product.name, product.price, product.in_stock, product.description])
        
        # Get the created product (find by name since it should be unique enough)
        rows = engine.select("products", columns=None, where_clause={"name": product.name})
        if rows:
            # If multiple found, get the one with matching price
            matching = [r for r in rows if r.get("price") == product.price]
            if matching:
                return ProductResponse(**matching[-1])  # Get the last one (newest)
            return ProductResponse(**rows[-1])  # Fallback to last one
        raise HTTPException(status_code=500, detail="Failed to retrieve created product")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/products/{product_id}", response_model=ProductResponse, tags=["Products"])
async def update_product(product_id: int, product: ProductUpdate):
    """Update a product by ID."""
    try:
        rows = engine.select("products", columns=None, where_clause={"id": product_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        assignments = {}
        if product.name is not None:
            assignments["name"] = product.name
        if product.price is not None:
            assignments["price"] = product.price
        if product.description is not None:
            assignments["description"] = product.description
        if product.in_stock is not None:
            assignments["in_stock"] = product.in_stock
        
        if assignments:
            engine.update("products", assignments, {"id": product_id})
        
        rows = engine.select("products", columns=None, where_clause={"id": product_id})
        return ProductResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/products/{product_id}", response_model=MessageResponse, tags=["Products"])
async def delete_product(product_id: int):
    """Delete a product by ID."""
    try:
        rows_deleted = engine.delete("products", {"id": product_id})
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        return MessageResponse(message=f"Product with ID {product_id} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Orders Endpoints
# ============================================================================

@app.get("/orders", response_model=List[OrderResponse], tags=["Orders"])
async def get_all_orders():
    """Get all orders."""
    try:
        rows = engine.select("orders", columns=None, where_clause=None)
        return [OrderResponse(**row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/orders/{order_id}", response_model=OrderResponse, tags=["Orders"])
async def get_order(order_id: int = Path(..., description="Order ID")):
    """Get a specific order by ID."""
    try:
        rows = engine.select("orders", columns=None, where_clause={"id": order_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Order with ID {order_id} not found")
        return OrderResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/orders", response_model=OrderResponse, status_code=201, tags=["Orders"])
async def create_order(order: OrderCreate):
    """Create a new order."""
    try:
        engine.insert("orders", [None, order.user_id, order.total, order.status])
        
        # Get all orders and find the one matching our criteria (should be the newest)
        all_orders = engine.select("orders", columns=None, where_clause=None)
        matching = [o for o in all_orders if o.get("user_id") == order.user_id and 
                    o.get("total") == order.total and o.get("status") == order.status]
        if matching:
            return OrderResponse(**matching[-1])  # Get the last one (newest)
        raise HTTPException(status_code=500, detail="Failed to retrieve created order")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/orders/{order_id}", response_model=OrderResponse, tags=["Orders"])
async def update_order(order_id: int, order: OrderUpdate):
    """Update an order by ID."""
    try:
        rows = engine.select("orders", columns=None, where_clause={"id": order_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Order with ID {order_id} not found")
        
        assignments = {}
        if order.user_id is not None:
            assignments["user_id"] = order.user_id
        if order.total is not None:
            assignments["total"] = order.total
        if order.status is not None:
            assignments["status"] = order.status
        if order.created_at is not None:
            assignments["created_at"] = order.created_at
        
        if assignments:
            engine.update("orders", assignments, {"id": order_id})
        
        rows = engine.select("orders", columns=None, where_clause={"id": order_id})
        return OrderResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/orders/{order_id}", response_model=MessageResponse, tags=["Orders"])
async def delete_order(order_id: int):
    """Delete an order by ID."""
    try:
        rows_deleted = engine.delete("orders", {"id": order_id})
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail=f"Order with ID {order_id} not found")
        return MessageResponse(message=f"Order with ID {order_id} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Order Items Endpoints
# ============================================================================

@app.get("/order-items", response_model=List[OrderItemResponse], tags=["Order Items"])
async def get_all_order_items():
    """Get all order items."""
    try:
        rows = engine.select("order_items", columns=None, where_clause=None)
        return [OrderItemResponse(**row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/order-items/{order_item_id}", response_model=OrderItemResponse, tags=["Order Items"])
async def get_order_item(order_item_id: int = Path(..., description="Order Item ID")):
    """Get a specific order item by ID."""
    try:
        rows = engine.select("order_items", columns=None, where_clause={"id": order_item_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Order item with ID {order_item_id} not found")
        return OrderItemResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/order-items/order/{order_id}", response_model=List[OrderItemResponse], tags=["Order Items"])
async def get_order_items_by_order(order_id: int = Path(..., description="Order ID")):
    """Get all order items for a specific order."""
    try:
        rows = engine.select("order_items", columns=None, where_clause={"order_id": order_id})
        return [OrderItemResponse(**row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/order-items", response_model=OrderItemResponse, status_code=201, tags=["Order Items"])
async def create_order_item(order_item: OrderItemCreate):
    """Create a new order item."""
    try:
        engine.insert("order_items", [None, order_item.order_id, order_item.product_id, order_item.quantity])
        
        # Get all order items and find the one matching our criteria (should be the newest)
        all_items = engine.select("order_items", columns=None, where_clause=None)
        matching = [item for item in all_items if 
                   item.get("order_id") == order_item.order_id and
                   item.get("product_id") == order_item.product_id and
                   item.get("quantity") == order_item.quantity]
        if matching:
            return OrderItemResponse(**matching[-1])  # Get the last one (newest)
        raise HTTPException(status_code=500, detail="Failed to retrieve created order item")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/order-items/{order_item_id}", response_model=OrderItemResponse, tags=["Order Items"])
async def update_order_item(order_item_id: int, order_item: OrderItemUpdate):
    """Update an order item by ID."""
    try:
        rows = engine.select("order_items", columns=None, where_clause={"id": order_item_id})
        if not rows:
            raise HTTPException(status_code=404, detail=f"Order item with ID {order_item_id} not found")
        
        assignments = {}
        if order_item.order_id is not None:
            assignments["order_id"] = order_item.order_id
        if order_item.product_id is not None:
            assignments["product_id"] = order_item.product_id
        if order_item.quantity is not None:
            assignments["quantity"] = order_item.quantity
        
        if assignments:
            engine.update("order_items", assignments, {"id": order_item_id})
        
        rows = engine.select("order_items", columns=None, where_clause={"id": order_item_id})
        return OrderItemResponse(**rows[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/order-items/{order_item_id}", response_model=MessageResponse, tags=["Order Items"])
async def delete_order_item(order_item_id: int):
    """Delete an order item by ID."""
    try:
        rows_deleted = engine.delete("order_items", {"id": order_item_id})
        if rows_deleted == 0:
            raise HTTPException(status_code=404, detail=f"Order item with ID {order_item_id} not found")
        return MessageResponse(message=f"Order item with ID {order_item_id} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
