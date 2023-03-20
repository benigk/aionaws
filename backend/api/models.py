from django.db import models
from django.utils.translation import gettext_lazy as _
# Create your models here.

class Field(models.Model):
    
    class Meta:
        app_label = 'api'
    
    class FieldList(models.TextChoices):
        LIFE_SCIENCE = 'LS', _('Life Science')
        BIOLOGY = 'BIO', _('Biology')
        MEDICINE = 'MED', _('Medicine')
    
    name = models.CharField(
        max_length=3,
        choices=FieldList.choices,
        blank=True,
        null=True
    )

    def __str__(self):
        return self.name

class Journal(models.Model):

    class Meta:
        app_label = 'api'
        
    
    name = models.CharField(max_length=512)
    impact_factor = models.FloatField(default=0.0)


class Country(models.Model):

    class Meta:
        app_label = 'api'
        
    
    coordinate_x = models.FloatField(blank=True, null=True)
    coordinate_y = models.FloatField(blank=True, null=True)
    name = models.CharField(max_length=128)
    # code = models.CharField(max_length=4, blank=True, null=True)

    def __str__(self):
        return self.name
    
    def get_coordinates(self):
        return [self.coordinate_x, self.coordinate_y]
    

# class City(Country):
#    question: can not be migrated by django because of reverse reference
#    country = models.ForeignKey(Country, on_delete=models.CASCADE)
    

class Paper(models.Model):
    
    class Meta:
        app_label = 'api'
        
    
    pmid = models.CharField(max_length=128)
    title = models.CharField(max_length=512)
    country = models.ManyToManyField(Country, through='PaperCountry')
    # city = models.ManyToManyField(City, through='PaperCity')
    journal = models.ForeignKey(Journal, on_delete=models.CASCADE) # if the journal is deleted, is it necessary to delete papers
    field = models.ForeignKey(Field, on_delete=models.CASCADE)

    def __str__(self):
        return self.title
    
    def get_impact_factor(self):
        pass

class PaperCountry(models.Model):

    class Meta:
        app_label = 'api'
    
    paper = models.ForeignKey(Paper, related_name='papercountries', on_delete=models.CASCADE)
    country = models.ForeignKey(Country, related_name='countrypapers', on_delete=models.CASCADE)
    
    def __str__(self):
        return f'{str(self.paper)} is published in {str(self.country)}'

# class PaperCity(models.Model):

#     class Meta:
#         app_label = 'api'
        
    
#     paper = models.ForeignKey(Paper, related_name='papercities', on_delete=models.CASCADE)
#     city = models.ForeignKey(City, related_name='citypapers', on_delete=models.CASCADE)
    
#     def __str__(self):
#         return f'{str(self.paper)} is published in {str(self.city)}'


class Author(models.Model):

    class Meta:
        app_label = 'api'
        
    
    first_name  = models.CharField(max_length=128)
    last_name   = models.CharField(max_length=128)
    fields      = models.ForeignKey(Field, on_delete=models.CASCADE)
    papers      = models.ManyToManyField(Paper, through='Ownership')
    country     = models.ManyToManyField(Country, through='AuthorCountry')
    # city        = models.ManyToManyField(City, through='AuthorCity')

    def __str__(self):
        return ' '.join([self.first_name, self.last_name])
    
    def get_paper_number(self):
        return len(self.papers)
    
    def get_impact_factor_by_field(self, field):
        """Given a field, to compute the impact factor of the author"""
        print(field)
        return 0

class Ownership(models.Model):
    
    class Meta:
    
        app_label = 'api'
        
    
    paper = models.ForeignKey(Paper, related_name='paperauthors', on_delete=models.CASCADE)
    author = models.ForeignKey(Author, related_name='authorpapers', on_delete=models.CASCADE)

    def __str__(self):
        return f'{str(self.paper)} is written by {str(self.author)}.'

class AuthorCountry(models.Model):

    class Meta:
        app_label = 'api'
        
    
    author = models.ForeignKey(Author, related_name='authorcountries', on_delete=models.CASCADE)
    country = models.ForeignKey(Country, related_name='countryauthors', on_delete=models.CASCADE)

    def __str__(self):
        return f'{str(self.author)} is from {str(self.country)}'
    

# class AuthorCity(models.Model):

#     class Meta:
#         app_label = 'api'
        

#     author = models.ForeignKey(Author, related_name='authorcities', on_delete=models.CASCADE)
#     city = models.ForeignKey(City, related_name='cityauthors', on_delete=models.CASCADE)

#     def __str__(self):
#         return f'{str(self.author)} is from {str(self.city)}'

