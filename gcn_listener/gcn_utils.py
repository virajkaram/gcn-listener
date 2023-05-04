# Taken from https://github.com/skyportal/skyportal/blob/main/
# services/gcn_service/gcn_service.py


import gcn
from astropy.time import Time
import os
import lxml
import xmlschema
from urllib.parse import urlparse


notice_types_dict = {150: 'LVC_PRELIMINARY',
                     151: 'LVC_INITIAL',
                     152: 'LVC_UPDATE',
                     153: 'LVC_TEST',
                     154: 'LVC_COUNTERPART',
                     163: 'LVC_EARLY_WARNING',
                     164: 'LVC_RETRACTION'
                     }

inv_notice_types_dict = {v: k for k, v in notice_types_dict.items()}


def get_root_from_payload(payload):
    schema = (
        f'{os.path.dirname(__file__)}/data/schema/VOEvent-v2.0.xsd'
    )
    voevent_schema = xmlschema.XMLSchema(schema)
    if voevent_schema.is_valid(payload):
        # check if is string
        try:
            payload = payload.encode('ascii')
        except AttributeError:
            pass
        root = lxml.etree.fromstring(payload)
    else:
        raise ValueError("xml file is not valid VOEvent")

    return root


def get_notice_type(root):
    return gcn.get_notice_type(root)


def get_trigger(root):
    """Get the trigger ID from a GCN notice."""

    property_name = "TrigID"
    path = f".//Param[@name='{property_name}']"
    elem = root.find(path)
    if elem is None:
        return None
    value = elem.attrib.get('value', None)
    if value is not None:
        value = int(value)

    return value


def get_dateobs(root):
    """Get the UTC event time from a GCN notice, rounded to the nearest second,
    as a datetime.datetime object."""
    dateobs = Time(
        root.find(
            "./WhereWhen/{*}ObsDataLocation"
            "/{*}ObservationLocation"
            "/{*}AstroCoords"
            "[@coord_system_id='UTC-FK5-GEO']"
            "/Time/TimeInstant/ISOTime"
        ).text,
        precision=0,
    )

    # FIXME: https://github.com/astropy/astropy/issues/7179
    dateobs = Time(dateobs.iso)

    return dateobs.datetime


def is_retraction(root):
    retraction = root.find("./What/Param[@name='Retraction']")
    if retraction is not None:
        retraction = int(retraction.attrib['value'])
        if retraction == 1:
            return True
    return False


def get_properties(root):

    property_names = [
        # Gravitational waves
        "HasNS",
        "HasRemnant",
        "FAR",
        "BNS",
        "NSBH",
        "BBH",
        "MassGap",
        "Terrestrial",
        # GRBs
        "Burst_Signif",
        "Data_Signif",
        "Det_Signif",
        "Image_Signif",
        "Rate_Signif",
        "Trig_Signif",
        "Burst_Inten",
        "Burst_Peak",
        "Data_Timescale",
        "Data_Integ",
        "Integ_Time",
        "Trig_Timescale",
        "Trig_Dur",
        "Hardness_Ratio",
        # Neutrinos
        "signalness",
        "energy",
    ]
    property_dict = {}
    for property_name in property_names:
        path = f".//Param[@name='{property_name}']"
        elem = root.find(path)
        if elem is None:
            continue
        value = elem.attrib.get('value', None)
        if value is not None:
            value = float(value)
            property_dict[property_name] = value

    return property_dict


def get_tags(root):
    """Get source classification tag strings from GCN notice."""
    # Get event stream.
    mission = urlparse(root.attrib['ivorn']).path.lstrip('/')
    yield mission

    # What type of burst is this: GRB or GW?
    try:
        value = root.find("./Why/Inference/Concept").text
    except AttributeError:
        pass
    else:
        if value == 'process.variation.burst;em.gamma':
            # Is this a GRB at all?
            try:
                value = root.find(".//Param[@name='GRB_Identified']").attrib['value']
            except AttributeError:
                yield 'GRB'
            else:
                if value == 'false':
                    yield 'Not GRB'
                else:
                    yield 'GRB'
        elif value == 'process.variation.trans;em.gamma':
            yield 'transient'

    # LIGO/Virgo alerts don't provide the Why/Inference/Concept tag,
    # so let's just identify it as a GW event based on the notice type.
    notice_type = gcn.get_notice_type(root)
    if notice_type in {
        gcn.NoticeType.LVC_PRELIMINARY,
        gcn.NoticeType.LVC_INITIAL,
        gcn.NoticeType.LVC_UPDATE,
        gcn.NoticeType.LVC_RETRACTION,
    }:
        yield 'GW'
    elif notice_type in {
        gcn.NoticeType.ICECUBE_ASTROTRACK_GOLD,
        gcn.NoticeType.ICECUBE_ASTROTRACK_BRONZE,
    }:
        yield 'Neutrino'
        yield 'IceCube'

    if notice_type == gcn.NoticeType.ICECUBE_ASTROTRACK_GOLD:
        yield 'Gold'
    elif notice_type == gcn.NoticeType.ICECUBE_ASTROTRACK_BRONZE:
        yield 'Bronze'

    # Is this a retracted LIGO/Virgo event?
    if notice_type == gcn.NoticeType.LVC_RETRACTION:
        yield 'retracted'

    # Is this a short GRB, or a long GRB?
    try:
        value = root.find(".//Param[@name='Long_short']").attrib['value']
    except AttributeError:
        pass
    else:
        if value != 'unknown':
            yield value.lower()

    # Gaaaaaah! Alerts of type FERMI_GBM_SUBTHRESH store the
    # classification in a different property!
    try:
        value = root.find(".//Param[@name='Duration_class']").attrib['value'].title()
    except AttributeError:
        pass
    else:
        if value != 'unknown':
            yield value.lower()

    # Get LIGO/Virgo source classification, if present.
    classifications = [
        (float(elem.attrib['value']), elem.attrib['name'])
        for elem in root.iterfind("./What/Group[@type='Classification']/Param")
    ]
    if classifications:
        _, classification = max(classifications)
        yield classification

    search = root.find("./What/Param[@name='Search']")
    if search is not None:
        yield search.attrib['value']

    # Get Instruments, if present.
    try:
        value = root.find(".//Param[@name='Instruments']").attrib['value']
    except AttributeError:
        pass
    else:
        instruments = value.split(",")
        yield from instruments
