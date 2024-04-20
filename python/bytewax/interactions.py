import json

from mimesis import Generic
from mimesis.locales import Locale
import random
from datetime import datetime, timedelta, timezone


def generate_live_interactions(num_interactions):
    """
    Generate a list of dictionaries, each representing an interaction between a user and a video.
    """
    DAY_FORMAT = '%Y-%m-%d'

    generic = Generic(locale=Locale.EN)
    interactions = []  # List to store generated interaction data

    categories = ['Education', 'Entertainment', 'Lifestyle', 'Music', 'News', 'Sports', 'Technology', 'Dance',
                  'Cooking', 'Comedy', 'Travel']
    interaction_types = ['like', 'dislike', 'view', 'comment', 'share', 'skip']

    for _ in range(num_interactions):
        interaction_date = datetime.now()

        # Constructing the interaction dictionary
        interaction = {
            'interaction_id': generic.person.identifier(mask='####-##-####'),
            'user_id': generic.person.identifier(mask='@@###@'),
            'video_id': generic.person.identifier(mask='#@@##@'),
            'video_category': random.choice(categories),
            'interaction_type': random.choice(interaction_types),
            'watch_time': random.randint(10, 250),
            'interaction_date': interaction_date,
            'interaction_day': interaction_date.strftime(DAY_FORMAT),
        }

        interactions.append(interaction)  # Add the interaction to the list

    return interactions
