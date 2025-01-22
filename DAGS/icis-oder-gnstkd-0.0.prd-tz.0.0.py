from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='gnstkd',WORKFLOW_ID='2fa58f6e05184c01aa94548fcab2b089', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-gnstkd-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 25, 6, 27, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2fa58f6e05184c01aa94548fcab2b089')

    tkdgns_vol = []
    tkdgns_volMnt = []
    tkdgns_vol.append(getVolume('shared-volume','shared-volume'))
    tkdgns_volMnt.append(getVolumeMount('shared-volume','test'))

    tkdgns_env = [getICISConfigMap('icis-oder-availabilitytest-configmap'), getICISConfigMap('icis-oder-availabilitytest-configmap2'), getICISSecret('icis-oder-availabilitytest-secret')]
    tkdgns_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    tkdgns_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    tkdgns = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ca19589c54294975a6b96dc078ab578c',
        'volumes': tkdgns_vol,
        'volume_mounts': tkdgns_volMnt,
        'env_from':tkdgns_env,
        'task_id':'tkdgns',
        'image':'/icis/icis-oder-availabilitytest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2fa58f6e05184c01aa94548fcab2b089')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tkdgns,
        Complete
    ]) 

    # authCheck >> tkdgns >> Complete
    workflow








