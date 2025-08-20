"""Validation models and data structures."""

from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel


class ValidationSeverity(str, Enum):
    """Validation issue severity levels."""
    ERROR = "error"
    WARNING = "warning" 
    INFO = "info"


@dataclass
class ValidationRule:
    """Represents a validation rule.
    
    Args:
        name: Unique rule identifier
        description: Human-readable description
        validate_func: Function that performs validation
        severity: Rule severity level
        category: Optional rule category
    """
    name: str
    description: str
    validate_func: Callable[[Any], List[Dict[str, Any]]]
    severity: str = "warning"
    category: Optional[str] = None
    
    def __post_init__(self):
        """Validate rule configuration."""
        if self.severity not in ["error", "warning", "info"]:
            raise ValueError(f"Invalid severity: {self.severity}")


class ValidationReport(BaseModel):
    """Comprehensive validation report.
    
    Contains all validation results including errors, warnings, and notices,
    along with an overall score and status.
    """
    status: str  # "valid", "valid_with_warnings", "invalid"
    score: float  # 0-100 validation score
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    notices: List[Dict[str, Any]] = field(default_factory=list)
    feed_path: Optional[str] = None
    validated_at: datetime = field(default_factory=datetime.now)
    
    @property
    def total_issues(self) -> int:
        """Total number of validation issues."""
        return len(self.errors) + len(self.warnings) + len(self.notices)
    
    @property
    def has_errors(self) -> bool:
        """Whether the report contains errors."""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Whether the report contains warnings."""
        return len(self.warnings) > 0
    
    def get_issues_by_rule(self, rule_name: str) -> List[Dict[str, Any]]:
        """Get all issues for a specific rule.
        
        Args:
            rule_name: Name of the validation rule
            
        Returns:
            List of issues for the specified rule
        """
        issues = []
        for issue_list in [self.errors, self.warnings, self.notices]:
            issues.extend([issue for issue in issue_list if issue.get('rule') == rule_name])
        return issues
    
    def get_issues_by_severity(self, severity: str) -> List[Dict[str, Any]]:
        """Get all issues of a specific severity.
        
        Args:
            severity: Issue severity ('error', 'warning', 'info')
            
        Returns:
            List of issues with the specified severity
        """
        if severity == "error":
            return self.errors.copy()
        elif severity == "warning":
            return self.warnings.copy()
        elif severity == "info":
            return self.notices.copy()
        else:
            return []
    
    def summary(self) -> Dict[str, Any]:
        """Generate validation summary.
        
        Returns:
            Dictionary with validation summary
        """
        return {
            "status": self.status,
            "score": round(self.score, 1),
            "total_issues": self.total_issues,
            "errors": len(self.errors),
            "warnings": len(self.warnings), 
            "notices": len(self.notices),
            "validated_at": self.validated_at.isoformat(),
            "feed_path": self.feed_path,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "status": self.status,
            "score": self.score,
            "errors": self.errors,
            "warnings": self.warnings,
            "notices": self.notices,
            "feed_path": self.feed_path,
            "validated_at": self.validated_at.isoformat(),
            "summary": self.summary(),
        }
        
    def to_json(self) -> str:
        """Convert report to JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


@dataclass
class ValidationIssue:
    """Represents a single validation issue.
    
    Args:
        rule: Rule that generated the issue
        message: Issue description
        severity: Issue severity level
        details: Additional issue details
        file_name: GTFS file where issue was found
        line_number: Line number where issue occurs (if applicable)
    """
    rule: str
    message: str
    severity: ValidationSeverity
    details: Dict[str, Any] = field(default_factory=dict)
    file_name: Optional[str] = None
    line_number: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert issue to dictionary."""
        return {
            "rule": self.rule,
            "message": self.message,
            "severity": self.severity.value,
            "details": self.details,
            "file_name": self.file_name,
            "line_number": self.line_number,
        }
