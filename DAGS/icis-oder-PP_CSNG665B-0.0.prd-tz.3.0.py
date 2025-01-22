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
                , WORKFLOW_NAME='PP_CSNG665B',WORKFLOW_ID='0060571a5aa2406886c9bb6198b8c1aa', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG665B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0060571a5aa2406886c9bb6198b8c1aa')

    csng665bJob_vol = []
    csng665bJob_volMnt = []
    csng665bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng665bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng665bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng665bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng665bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng665bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '97fc79880dc1483b97703de5bb1d8e75',
        'volumes': csng665bJob_vol,
        'volume_mounts': csng665bJob_volMnt,
        'env_from':csng665bJob_env,
        'task_id':'csng665bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng665bJob", "fromDate=20240601", "toDate=20240628", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0060571a5aa2406886c9bb6198b8c1aa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng665bJob,
        Complete
    ]) 

    # authCheck >> csng665bJob >> Complete
    workflow








