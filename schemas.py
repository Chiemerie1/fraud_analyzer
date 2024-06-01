from datetime import datetime
from pydantic import BaseModel, conint, confloat, constr, conlist
import string
import random
from typing import List, Optional




def random_alphanum():
    code = random.choices(string.ascii_letters + string.digits, k=6)
    return "".join(code)



class UserBase(BaseModel):
    username: str
    company_id: str = random_alphanum()
    
    
class UserCreate(UserBase):
    password: str
    
    
class User(UserBase):
    id: int
    is_active: bool
    
    class Config:
        orm_mode = True

######################################################
class TransTypeConfig(BaseModel):
    max_amount: conint(gt=0)
    risk_tolerance: confloat(gt=0)
    type: constr(to_upper=True, min_length=3)
    rules: conlist(item_type=str) = []
    
class Tier(BaseModel):
    id: int
    transaction_types: conlist(item_type=TransTypeConfig, min_items=1)
    
    def get_trans_type_config(self, type: str)->TransTypeConfig | None:
        trans_types: List[TransTypeConfig] = self.transaction_types
        
        for x in trans_types:
            if x.type.upper() == type:
                return x
        return None
        
    
class CompanyConfig(BaseModel):
    risk_appetite: confloat(gt=0)
    tier_config: conlist(item_type=Tier, min_items=1)
    high_risk_period_start: int
    high_risk_period_end: int
    
    def get_tier(self, id: int) -> Tier | None:        
        tiers: List[Tier] = self.tier_config
        
        for tier in tiers:
            if tier.id == id:
                return tier
            
                
    
class Company(BaseModel):
    # id: int | None
    name: constr(to_upper=True, min_length=3)
    configuration: CompanyConfig
    ulid: constr(to_upper=True, min_length=3) | None
    
    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        
        

class CustomerTransaction(BaseModel):
    hash: str | None
    type: str
    amount: conint(gt=0)
    customer_tier: int
    customer_unique_id: str
    transaction_time: datetime
    company_id: str
    
