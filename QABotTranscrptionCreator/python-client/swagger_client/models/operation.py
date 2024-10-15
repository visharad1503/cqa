# coding: utf-8

"""
    Speech Services API version 3.2

    Speech Services API version 3.2.  # noqa: E501

    OpenAPI spec version: 3.2
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""


import pprint
import re  # noqa: F401

import six

from swagger_client.configuration import Configuration


class Operation(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'id': 'str',
        'result': 'OperationResult',
        'error': 'EntityError',
        '_self': 'str',
        'last_action_date_time': 'datetime',
        'status': 'Status',
        'created_date_time': 'datetime'
    }

    attribute_map = {
        'id': 'id',
        'result': 'result',
        'error': 'error',
        '_self': 'self',
        'last_action_date_time': 'lastActionDateTime',
        'status': 'status',
        'created_date_time': 'createdDateTime'
    }

    def __init__(self, id=None, result=None, error=None, _self=None, last_action_date_time=None, status=None, created_date_time=None, _configuration=None):  # noqa: E501
        """Operation - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._id = None
        self._result = None
        self._error = None
        self.__self = None
        self._last_action_date_time = None
        self._status = None
        self._created_date_time = None
        self.discriminator = None

        self.id = id
        if result is not None:
            self.result = result
        if error is not None:
            self.error = error
        if _self is not None:
            self._self = _self
        if last_action_date_time is not None:
            self.last_action_date_time = last_action_date_time
        if status is not None:
            self.status = status
        if created_date_time is not None:
            self.created_date_time = created_date_time

    @property
    def id(self):
        """Gets the id of this Operation.  # noqa: E501

        The identifier of this Operation.  # noqa: E501

        :return: The id of this Operation.  # noqa: E501
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        """Sets the id of this Operation.

        The identifier of this Operation.  # noqa: E501

        :param id: The id of this Operation.  # noqa: E501
        :type: str
        """
        if self._configuration.client_side_validation and id is None:
            raise ValueError("Invalid value for `id`, must not be `None`")  # noqa: E501

        self._id = id

    @property
    def result(self):
        """Gets the result of this Operation.  # noqa: E501


        :return: The result of this Operation.  # noqa: E501
        :rtype: OperationResult
        """
        return self._result

    @result.setter
    def result(self, result):
        """Sets the result of this Operation.


        :param result: The result of this Operation.  # noqa: E501
        :type: OperationResult
        """

        self._result = result

    @property
    def error(self):
        """Gets the error of this Operation.  # noqa: E501


        :return: The error of this Operation.  # noqa: E501
        :rtype: EntityError
        """
        return self._error

    @error.setter
    def error(self, error):
        """Sets the error of this Operation.


        :param error: The error of this Operation.  # noqa: E501
        :type: EntityError
        """

        self._error = error

    @property
    def _self(self):
        """Gets the _self of this Operation.  # noqa: E501

        The location of this entity.  # noqa: E501

        :return: The _self of this Operation.  # noqa: E501
        :rtype: str
        """
        return self.__self

    @_self.setter
    def _self(self, _self):
        """Sets the _self of this Operation.

        The location of this entity.  # noqa: E501

        :param _self: The _self of this Operation.  # noqa: E501
        :type: str
        """

        self.__self = _self

    @property
    def last_action_date_time(self):
        """Gets the last_action_date_time of this Operation.  # noqa: E501

        The time-stamp when the current status was entered.  The time stamp is encoded as ISO 8601 date and time format  (\"YYYY-MM-DDThh:mm:ssZ\", see https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations).  # noqa: E501

        :return: The last_action_date_time of this Operation.  # noqa: E501
        :rtype: datetime
        """
        return self._last_action_date_time

    @last_action_date_time.setter
    def last_action_date_time(self, last_action_date_time):
        """Sets the last_action_date_time of this Operation.

        The time-stamp when the current status was entered.  The time stamp is encoded as ISO 8601 date and time format  (\"YYYY-MM-DDThh:mm:ssZ\", see https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations).  # noqa: E501

        :param last_action_date_time: The last_action_date_time of this Operation.  # noqa: E501
        :type: datetime
        """

        self._last_action_date_time = last_action_date_time

    @property
    def status(self):
        """Gets the status of this Operation.  # noqa: E501


        :return: The status of this Operation.  # noqa: E501
        :rtype: Status
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of this Operation.


        :param status: The status of this Operation.  # noqa: E501
        :type: Status
        """

        self._status = status

    @property
    def created_date_time(self):
        """Gets the created_date_time of this Operation.  # noqa: E501

        The time-stamp when the object was created.  The time stamp is encoded as ISO 8601 date and time format  (\"YYYY-MM-DDThh:mm:ssZ\", see https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations).  # noqa: E501

        :return: The created_date_time of this Operation.  # noqa: E501
        :rtype: datetime
        """
        return self._created_date_time

    @created_date_time.setter
    def created_date_time(self, created_date_time):
        """Sets the created_date_time of this Operation.

        The time-stamp when the object was created.  The time stamp is encoded as ISO 8601 date and time format  (\"YYYY-MM-DDThh:mm:ssZ\", see https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations).  # noqa: E501

        :param created_date_time: The created_date_time of this Operation.  # noqa: E501
        :type: datetime
        """

        self._created_date_time = created_date_time

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(Operation, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, Operation):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, Operation):
            return True

        return self.to_dict() != other.to_dict()
