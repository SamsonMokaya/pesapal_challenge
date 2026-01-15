"""
Pydantic models for the RDBMS API.
Defines request and response models for each table.
"""

from pydantic import BaseModel, Field
from typing import Optional, List


# ============================================================================
# Generic Response Models
# ============================================================================

class MessageResponse(BaseModel):
    """Generic message response."""
    message: str
    status: str = "success"


class CountResponse(BaseModel):
    """Response with count."""
    count: int


# ============================================================================
# Pydantic Models for Users
# ============================================================================

class UserCreate(BaseModel):
    """Model for creating a user."""
    name: str = Field(..., description="User's full name")
    email: str = Field(..., description="User's email address (must be unique)")
    age: Optional[int] = Field(None, description="User's age")


class UserUpdate(BaseModel):
    """Model for updating a user."""
    name: Optional[str] = Field(None, description="User's full name")
    email: Optional[str] = Field(None, description="User's email address")
    age: Optional[int] = Field(None, description="User's age")


class UserResponse(BaseModel):
    """Response model for a user."""
    id: int
    name: str
    email: str
    age: Optional[int] = None


# ============================================================================
# Pydantic Models for Products
# ============================================================================

class ProductCreate(BaseModel):
    """Model for creating a product."""
    name: str = Field(..., description="Product name")
    price: float = Field(..., description="Product price")
    description: Optional[str] = Field(None, description="Product description")
    in_stock: Optional[bool] = Field(None, description="Whether product is in stock")


class ProductUpdate(BaseModel):
    """Model for updating a product."""
    name: Optional[str] = Field(None, description="Product name")
    price: Optional[float] = Field(None, description="Product price")
    description: Optional[str] = Field(None, description="Product description")
    in_stock: Optional[bool] = Field(None, description="Whether product is in stock")


class ProductResponse(BaseModel):
    """Response model for a product."""
    id: int
    name: str
    price: float
    description: Optional[str] = None
    in_stock: Optional[bool] = None


# ============================================================================
# Pydantic Models for Orders
# ============================================================================

class OrderCreate(BaseModel):
    """Model for creating an order."""
    user_id: int = Field(..., description="ID of the user who placed the order")
    total: float = Field(..., description="Total order amount")
    status: str = Field(..., description="Order status (e.g., 'pending', 'completed')")
    created_at: Optional[str] = Field(None, description="Order creation date")


class OrderUpdate(BaseModel):
    """Model for updating an order."""
    user_id: Optional[int] = Field(None, description="ID of the user who placed the order")
    total: Optional[float] = Field(None, description="Total order amount")
    status: Optional[str] = Field(None, description="Order status")
    created_at: Optional[str] = Field(None, description="Order creation date")


class OrderResponse(BaseModel):
    """Response model for an order."""
    id: int
    user_id: int
    total: float
    status: str
    created_at: Optional[str] = None


# ============================================================================
# Pydantic Models for Order Items
# ============================================================================

class OrderItemCreate(BaseModel):
    """Model for creating an order item."""
    order_id: int = Field(..., description="ID of the order")
    product_id: int = Field(..., description="ID of the product")
    quantity: int = Field(..., description="Quantity of the product")


class OrderItemUpdate(BaseModel):
    """Model for updating an order item."""
    order_id: Optional[int] = Field(None, description="ID of the order")
    product_id: Optional[int] = Field(None, description="ID of the product")
    quantity: Optional[int] = Field(None, description="Quantity of the product")


class OrderItemResponse(BaseModel):
    """Response model for an order item."""
    id: int
    order_id: int
    product_id: int
    quantity: int
