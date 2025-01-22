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
                , WORKFLOW_NAME='PP_CSNG207B',WORKFLOW_ID='3671278e9afb44f998b9626452e32c62', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG207B-0.0.prd-tz.4.4'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 15, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3671278e9afb44f998b9626452e32c62')

    csng207bJob_vol = []
    csng207bJob_volMnt = []
    csng207bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng207bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng207bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng207bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng207bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng207bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '67cc332c890f44c8a8c8d729c4b958e8',
        'volumes': csng207bJob_vol,
        'volume_mounts': csng207bJob_volMnt,
        'env_from':csng207bJob_env,
        'task_id':'csng207bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng207bJob", "endTranDate=20240530", "empNo=952794853","ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3671278e9afb44f998b9626452e32c62')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng207bJob,
        Complete
    ]) 

    # authCheck >> csng207bJob >> Complete
    workflow








