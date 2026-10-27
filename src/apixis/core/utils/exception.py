class EventHandlerNotRegisteredError(Exception):
    
    def __init__(self, message="Invalid handler detected", errors=None):
        """        
        Args:
            message: error message
            errors: error object
        """
        self.message = message
        self.errors = errors if errors else []
        super().__init__(self.message)
    
    def __str__(self):
        error_details = f"Errors: {self.errors}" if self.errors else ""
        return f"{self.message}; {error_details}"
    

class EventHandlerAlreadyRegisteredError(Exception):
    
    def __init__(self, message="Handler already registered.", errors=None):
        """        
        Args:
            message: error message
            errors: error object
        """
        self.message = message
        self.errors = errors if errors else []
        super().__init__(self.message)
    
    def __str__(self):
        error_details = f"Errors: {self.errors}" if self.errors else ""
        return f"{self.message}; {error_details}"


class BlockHookNotRegisteredError(RuntimeError):
    """Raised when a graph emits a Block without a registered interruption hook."""


class BlockNotResolvedError(RuntimeError):
    """Raised when a graph emits a Block and not be resolved by any registered interruption hook."""
    

class GraphNodeError(Exception):
    
    def __init__(self, message="Graph node error", errors=None):
        """        
        Args:
            message: error message
            errors: error object
        """
        self.message = message
        self.errors = errors if errors else []
        super().__init__(self.message)
    
    def __str__(self):
        error_details = f"Errors: {self.errors}" if self.errors else ""
        return f"{self.message}; {error_details}"
    

class InvalidNodeReturnsError(Exception):
    
    def __init__(self, message="Invalid node returns", errors=None):
        """        
        Args:
            message: error message
            errors: error object
        """
        self.message = message
        self.errors = errors if errors else []
        super().__init__(self.message)
    
    def __str__(self):
        error_details = f"Errors: {self.errors}" if self.errors else ""
        return f"{self.message}; {error_details}"
    

class InvalidContextError(Exception):
    
    def __init__(self, message="Invalid context provided", errors=None):
        """        
        Args:
            message: error message
            errors: error object
        """
        self.message = message
        self.errors = errors if errors else []
        super().__init__(self.message)
    
    def __str__(self):
        error_details = f"Errors: {self.errors}" if self.errors else ""
        return f"{self.message}; {error_details}"
