"""
Pipeline Tests
==============
Smoke tests for the NLP processing pipeline.

Run:
    pytest scraper/tests/test_pipeline.py -v
"""

import pytest
from scraper.pipeline.profile_processor import ProfileProcessor


# ============================================================
# Profile Processor Smoke Test
# ============================================================

@pytest.mark.slow
def test_profile_processor_smoke():
    """
    End-to-end smoke test: process a known-good professor page
    and verify all expected fields are returned.
    """
    processor = ProfileProcessor()
    
    # Known-good page with clear research interests
    test_url = "https://www.uottawa.ca/faculty-science/professors/rafal-kulik"
    test_name = "Rafal Kulik"
    test_dept = "Department of Mathematics and Statistics"
    
    result = processor.process_profile(test_url, test_name, test_dept)
    
    # Should return a non-empty dict
    assert result, f"process_profile returned empty result for {test_url}"
    
    # Email should be found
    assert "email" in result, "Missing 'email' key in result"
    assert result["email"] is not None, "Email should not be None for this professor"
    assert "@" in result["email"], f"Invalid email format: {result['email']}"
    
    # Holistic string should be well-formed
    assert "holistic_profile_string" in result, "Missing 'holistic_profile_string'"
    holistic = result["holistic_profile_string"]
    assert test_name in holistic, f"Professor name not in holistic string: {holistic}"
    assert test_dept in holistic, f"Department not in holistic string: {holistic}"
    assert "Research interests:" in holistic, f"Missing 'Research interests:' in holistic string"
    
    # Should not contain empty research interests
    assert '""' not in holistic, f"Holistic string has empty research interests: {holistic}"
