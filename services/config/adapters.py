from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from allauth.exceptions import ImmediateHttpResponse
from django.http import HttpResponseForbidden
from django.contrib import messages
from django.conf import settings
import jwt
import logging

logger = logging.getLogger(__name__)


class CustomSocialAccountAdapter(DefaultSocialAccountAdapter):
    def is_open_for_signup(self, request, sociallogin):
        """
        Checks whether or not the site is open for signups.
        """
        return True
    
    def pre_social_login(self, request, sociallogin):
        """
        Called just before a user is logged in via social authentication.
        Validates that the user belongs to an allowed Keycloak group.
        """
        # Get allowed groups from settings
        allowed_groups = getattr(settings, 'KEYCLOAK_ALLOWED_GROUPS', [])
        
        # If no groups configured, allow all users
        if not allowed_groups:
            logger.warning("KEYCLOAK_ALLOWED_GROUPS not configured - allowing all users")
            return super().pre_social_login(request, sociallogin)
        
        # Extract groups from the ID token
        user_groups = self._extract_groups_from_token(sociallogin)
        
        logger.info(f"User attempting login with groups: {user_groups}")
        logger.info(f"Allowed groups: {allowed_groups}")
        
        # Check if user has at least one allowed group
        if not any(group in allowed_groups for group in user_groups):
            logger.warning(
                f"Login rejected: User groups {user_groups} do not match allowed groups {allowed_groups}"
            )
            messages.error(
                request,
                "Access denied. You must be a member of an authorized group to access this application."
            )
            raise ImmediateHttpResponse(
                HttpResponseForbidden(
                    "Access denied. You must be a member of an authorized group to access this application."
                )
            )
        
        logger.info(f"Login approved: User has valid group membership")
        return super().pre_social_login(request, sociallogin)
    
    def _extract_groups_from_token(self, sociallogin):
        """
        Extracts the groups claim from the Keycloak ID token.
        
        Args:
            sociallogin: The SocialLogin object containing token information
            
        Returns:
            list: List of group names the user belongs to
        """
        groups = []
        
        try:
            # Get the ID token from the sociallogin object
            token = sociallogin.token
            
            # The ID token is typically stored in token.token
            id_token = getattr(token, 'token', None)
            
            if not id_token:
                logger.error("No ID token found in sociallogin.token")
                return groups
            
            # Decode the JWT without verification
            # (Token is already verified by django-allauth during OAuth flow)
            decoded_token = jwt.decode(
                id_token,
                options={"verify_signature": False}
            )
            
            # Extract groups from the 'groups' claim
            groups_claim_name = settings.KEYCLOAK_GROUPS_CLAIM_NAME
            groups = decoded_token.get(groups_claim_name, [])
            
            # Ensure groups is always a list
            if not isinstance(groups, list):
                groups = [groups] if groups else []
            
            logger.debug(f"Decoded token claims: {list(decoded_token.keys())}")
            logger.debug(f"Extracted groups: {groups}")
            
        except jwt.DecodeError as e:
            logger.error(f"Failed to decode ID token: {e}")
        except Exception as e:
            logger.error(f"Unexpected error extracting groups from token: {e}")
        
        return groups
